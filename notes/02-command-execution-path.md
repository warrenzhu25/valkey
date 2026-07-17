# 02 — Command Execution Path

Trace one `GET foo` from socket to reply. If you learn one path in this codebase, learn
this one — every feature hooks into it somewhere.

## The path

```
socket readable
  └─ readQueryFromClient              networking.c:4341
       (may be offloaded to an I/O thread — see design-docs/io-threads.md)
       └─ processInputBuffer          networking.c:4203
            parses RESP into c->argv / c->argc
            └─ processCommandAndResetClient   networking.c:3932
                 └─ processCommand    server.c:4315   <-- ALL the gatekeeping
                      └─ call         server.c:3875   <-- actual execution
                           └─ c->cmd->proc(c)          e.g. getCommand
                                └─ addReply...          append to reply buffer
                 └─ commandProcessed  networking.c:3879
                      └─ propagate to AOF + replicas
                 └─ resetClient       networking.c:3365   frees argv, clears per-cmd state

... later, same loop iteration ...
beforeSleep  server.c:1854
  └─ flush reply buffers to sockets
```

## `processCommand` — the gatekeeper (`server.c:4315`)

This is the single most consequential function in the server. It does **not** execute
anything; it decides whether execution is allowed. The checks run in a **specific order**,
and the order is a feature — cheap/safety-critical checks first, so an unauthorized or
misrouted command is rejected before any expensive work. Reading top to bottom
(`server.c:4315` onward) you hit them in roughly this sequence:

1. **Command lookup + arity** — `lookupCommand`; does it exist, right number of args?
2. **Authentication** — `authRequired` (`~server.c:4361`); reject with `-NOAUTH` if not authed.
3. **`CMD_NO_MULTI`** — a few commands (e.g. `SUBSCRIBE`, `RESET`) can't be queued in a transaction.
4. **ACL** — `ACLCheckAllPerm` (`~server.c:4432`): may *this user* run *this command* on
   *these keys*? Reject with `-NOPERM`.
5. **Cluster redirection** — `getNodeByQuery` (`cluster.c:1048`, called `~server.c:4457`):
   do we own the slot for these keys? If not, `clusterRedirectClient` replies `-MOVED` /
   `-ASK` / `-CROSSSLOT` and we stop. (See note 08.)
6. **maxmemory** — `performEvictions` (`~server.c:4551`, note 05): if over the limit, try
   to evict; if we can't free enough and the command is `denyoom`, reject with `-OOM`.
7. **Persistence errors** — if the last bgsave failed and `stop-writes-on-bgsave-error`
   is set, reject writes (`~server.c:4589`).
8. **`min-replicas-to-write`** — `checkGoodReplicasStatus` (`~server.c:4613`): reject writes
   if too few replicas are online.
9. **Read-only replica** — `server.repl_replica_ro` (`~server.c:4620`): a replica rejects
   writes from normal clients.
10. **Pub/Sub mode (RESP2), loading state, cluster-down, busy script, MULTI queuing...**

If any check fails it calls one of the `rejectCommand*` helpers (`server.c:4143`–`4180`)
and returns *without* calling `call()`. This is why "why did my command get rejected?" is
nearly always answered by reading `processCommand` top to bottom — and the error string
itself (`-NOPERM`, `-MOVED`, `-OOM`, `-READONLY`) tells you which check fired.

**MULTI/EXEC note:** if the client is in a transaction, `processCommand` queues the
command instead of executing it. `EXEC` later calls `call()` on each queued command.

## `call` — the execution wrapper (`server.c:3875`)

By the time you're here, the command *is* going to run. `call(c, flags)` is the wrapper
that makes execution observable and durable. The `flags` argument (`server.h:569`) controls
propagation: `CMD_CALL_PROPAGATE_AOF`, `CMD_CALL_PROPAGATE_REPL`, `CMD_CALL_FROM_MODULE`,
with `CMD_CALL_FULL` = both propagation bits (the normal top-level case). A module's
`RM_Call` can pass `CMD_CALL_NONE` to run a command with propagation suppressed.

What `call()` does around the actual `c->cmd->proc(c)`:

- **Resets the per-command propagation flags** up front (`server.c:3895`): `force_aof`,
  `force_repl`, `prevent_prop`. A command sets these *during* execution to override the
  default propagation decision (e.g. a read that lazily-expired a key forces a `DEL` to
  propagate; a no-op write sets `prevent_prop`).
- Records start time via `ustime()` / `getMonotonicUs()`, runs the proc, records duration.
- Tracks `server.dirty` before/after — the delta is "did this command actually change the
  dataset?", which drives both propagation and the save-param counters (note 01).
- Updates stats: calls, microseconds, errors; feeds the **slowlog** and **latency monitor**.
- Fires **keyspace notifications**.
- Accumulates what should be **propagated** to the AOF and replicas.
- Handles **command tracking / client-side caching** invalidation.
- Runs inside an **execution unit** (`enterExecutionUnit`, `server.c:3912`) — see below.

### Propagation is not "log the command you ran"

This trips everyone up. Valkey often propagates something *different* from what the
client sent, because commands must be **deterministic** on replay. `SPOP` propagates as
`SREM` with the specific member it popped. `EXPIRE` propagates as `PEXPIREAT` with an
absolute timestamp. `INCRBYFLOAT` propagates as `SET`.

The mechanism has two forms:
- **Rewrite in place** — `rewriteClientCommandVector` (`networking.c:6019`) /
  `rewriteClientCommandArgument` (`networking.c:6059`) replace `c->argv`, so the *same*
  command slot propagates in its deterministic form.
- **Also-propagate** — `alsoPropagate` (`server.c:3680`) queues *additional* commands
  (e.g. one command that touches many keys emitting several `DEL`s).

Both feed a per-execution-unit buffer that `propagatePendingCommands` (`server.c:3746`)
flushes to the AOF and replicas at the end.

### Execution units — why propagation is deferred, not immediate

`call()` can **recurse**: a Lua script, a module `RM_Call`, or `EXEC` re-enters `call()`
for each inner command. Valkey wraps the whole outermost operation in an *execution unit*
and only flushes propagation when the unit finishes — `postExecutionUnitOperations`
(`server.c:3799`). The payoff: everything a script does propagates as one atomic block
(wrapped in `MULTI`/`EXEC` on the replica), so a replica can never observe a script
half-applied. This is the same reason keyspace notifications and client-side-cache
invalidations are batched to unit end.

If you have ever wondered how a replica stays byte-identical to its primary despite
random, time-dependent, or scripted commands: this is the mechanism.

## Where the command table lives

Commands are defined in JSON under `src/commands/*.json` and code-generated into
`commands.def`. If you add a command, you edit the JSON and regenerate (`utils/generate-command-code.py`,
run via `make`) — don't hand-edit `commands.def`. The `struct serverCommand` (in `server.h`)
carries the proc pointer, arity, flags (`write`, `readonly`, `denyoom`, `admin`...), and
**key specs** (which argv positions are keys). Those key specs are exactly what ACL (check 4)
and cluster redirection (check 5) consult to know which arguments to treat as keys — one
declaration, consumed by multiple gatekeeping stages.

## Exercise

Add a `serverLog(LL_WARNING, "call %s depth=%d", c->cmd->fullname, server.execution_nesting)`
at the top of `call()` (`server.c:3875`), rebuild, and run three things: a plain `GET`, a
`MULTI/SET/SET/EXEC`, and `EVAL "redis.call('set',KEYS[1],'x'); redis.call('incr',KEYS[2])" 2 a b`.
Watch the nesting: the plain command logs once at depth 0; `EXEC` logs the wrapper then each
queued command; the script logs once for `EVAL` then once per inner `redis.call`. Seeing how
many times `call()` re-enters for one client request — and that they all flush propagation
together at unit end — tells you more about the architecture than reading it does.

## Read next

Note 03 — the `client` struct those `addReply` calls are filling.
