# 02 — Command Execution Path

Every feature in Valkey — replication, scripting, ACLs, cluster redirection, keyspace
notifications — hooks into the same narrow path that a plain `GET` travels: bytes arrive on
a socket, get parsed into arguments, pass a gauntlet of checks, execute, and leave a reply
in a buffer. If you learn one path in this codebase, learn this one, because the others are
variations on it. This chapter follows a command from the socket read to the buffered
reply, and pays special attention to the two functions that do the heavy lifting:
`processCommand`, which decides whether a command is *allowed* to run, and `call`, which
runs it and makes the result durable and observable.

## The path at a glance

```
socket readable
  └─ readQueryFromClient            networking.c:4341
       (may be offloaded to an I/O thread — see design-docs/io-threads.md)
       └─ processInputBuffer        networking.c:4203
            parses RESP into c->argv / c->argc
            └─ processCommandAndResetClient   networking.c:3932
                 └─ processCommand   server.c:4315   <-- ALL the gatekeeping
                      └─ call        server.c:3875   <-- actual execution
                           └─ c->cmd->proc(c)         e.g. getCommand, spopCommand
                                └─ addReply...         append to the reply buffer
                 └─ commandProcessed  networking.c:3879
                      └─ propagate to the AOF + replicas
                 └─ resetClient       networking.c:3365   frees argv, clears per-cmd state

... later, same loop iteration (chapter 01) ...
beforeSleep  server.c:1854
  └─ flush reply buffers to sockets
```

The parsing step (`processInputBuffer`) turns wire bytes into a `c->argv` array of `robj`
strings. A RESP request like `SPOP myset` arrives as:

```
*2\r\n$4\r\nSPOP\r\n$5\r\nmyset\r\n
 │      │            │
 │      │            └ second bulk string: "myset"
 │      └ first bulk string: "SPOP"
 └ array of 2 elements
```

and comes out as `c->argv = ["SPOP", "myset"]`, `c->argc = 2`. From here on the command is
just those arguments plus the client state; the raw bytes are gone.

## `processCommand` — the gatekeeper (`server.c:4315`)

This is the single most consequential function in the server, and the key to understanding
it is that **it does not execute anything.** It decides whether execution is *allowed*. The
checks run in a deliberate order — cheap and safety-critical first — so that an
unauthenticated, misrouted, or forbidden command is rejected before the server spends any
effort on it. Reading `processCommand` top to bottom, you meet them roughly like this:

1. **Command lookup + arity** — `lookupCommand`: does this command exist, with a valid
   argument count? An unknown command or wrong arity is rejected here.
2. **Authentication** (`authRequired`, `server.c:4361`) — if the connection isn't
   authenticated and the command isn't one of the few allowed pre-auth (`AUTH`, `HELLO`),
   reject with `-NOAUTH`.
3. **`CMD_NO_MULTI`** (`server.c:4425`) — a handful of commands (`SUBSCRIBE`, `RESET`, ...)
   cannot be queued inside a `MULTI` transaction.
4. **ACL** (`ACLCheckAllPerm`, `server.c:4432`) — may *this user* run *this command* on
   *these keys and channels*? On failure, `-NOPERM`.
5. **Cluster redirection** (`getNodeByQuery`, `server.c:4457`) — in cluster mode, do we own
   the slot for these keys? If another node owns it, reply `-MOVED`; if the slot is
   mid-migration, `-ASK`; if the keys span slots, `-CROSSSLOT`. Execution stops (chapter 08).
6. **maxmemory** (`performEvictions`, `server.c:4551`) — if we're over the limit, try to
   evict; if we can't free enough and the command is flagged `denyoom`, reject with `-OOM`
   (chapter 05).
7. **Persistence errors** (`server.c:4589`) — if the last bgsave failed and
   `stop-writes-on-bgsave-error` is on, reject writes so you notice the disk problem.
8. **`min-replicas-to-write`** (`checkGoodReplicasStatus`, `server.c:4613`) — reject writes
   if too few replicas are currently online.
9. **Read-only replica** (`server.repl_replica_ro`, `server.c:4620`) — a replica rejects
   writes from normal clients with `-READONLY`.
10. **Pub/Sub-mode restrictions (RESP2), loading state, cluster-down, busy script, MULTI
    queuing...** — the long tail.

Every failure calls one of the `rejectCommand*` helpers (`server.c` around 4143–4180) and
returns **without ever calling `call()`**. This is why the answer to "why did my command get
rejected?" is almost always found by reading `processCommand` top to bottom — and the error
string itself (`-NOAUTH`, `-NOPERM`, `-MOVED`, `-OOM`, `-READONLY`) names the exact check
that fired.

**MULTI/EXEC:** if the client is inside a transaction, `processCommand` *queues* the command
rather than executing it. The queued commands run later, when `EXEC` calls `call()` on each
in turn.

## `call` — the execution wrapper (`server.c:3875`)

By the time control reaches `call`, the command *is* going to run. `call(c, flags)` is the
wrapper that turns "run this proc" into "run it, and make it durable, observable, and
replicated." The `flags` argument controls propagation: `CMD_CALL_PROPAGATE_AOF`,
`CMD_CALL_PROPAGATE_REPL`, and `CMD_CALL_FULL` (both bits set — the normal top-level case). A
module's `RM_Call` can pass `CMD_CALL_NONE` to run a command with propagation suppressed.

Around the single line that actually runs the command (`c->cmd->proc(c)`), `call` does all of
this:

- **Resets the per-command propagation flags** up front (`server.c:3895`): `force_aof`,
  `force_repl`, `prevent_prop`. A command sets these *during* execution to override the
  default propagation decision — a read that lazily-expired a key forces a `DEL` to
  propagate; a write that turned out to be a no-op sets `prevent_prop`.
- **Times the execution** — records `ustime()` / `getMonotonicUs()` before and after, which
  feeds the slowlog and the latency monitor.
- **Tracks `server.dirty`** before and after (`server.c:3907`). The delta answers "did this
  command actually change the dataset?" — and that single number drives both propagation
  (only dirtying commands propagate) and the `save <sec> <changes>` counters (chapter 01).
- **Updates stats** — per-command call count, total microseconds, error count.
- **Fires keyspace notifications** (the `NOTIFY_*` events).
- **Accumulates what should be propagated** to the AOF and replicas.
- **Handles client-side-caching invalidation** for tracked keys.
- **Enters an execution unit** — `enterExecutionUnit(1, call_timer)` (`server.c:3912`); see
  below.

### Propagation is not "log the command you ran"

This is the idea that trips everyone up, and the worked example below makes it concrete.
Valkey frequently propagates something *different* from what the client sent, because the
thing sent to replicas and the AOF must be **deterministic on replay**. If a replica ran the
literal command, a random or time-dependent command would produce a different result there
than on the primary, and the two would silently diverge. So:

- `SPOP` (pops a *random* member) propagates as `SREM` naming the specific member popped.
- `EXPIRE key 100` (relative to *now*) propagates as `PEXPIREAT key <absolute-ms>`.
- `INCRBYFLOAT` (floating-point rounding) propagates as `SET` with the exact resulting value.

There are two mechanisms:

- **Rewrite in place** — `rewriteClientCommandVector` (`networking.c` ~6019) /
  `rewriteClientCommandArgument` replace `c->argv` itself, so the command's *own* slot
  propagates in deterministic form.
- **Also-propagate** — `alsoPropagate` (`server.c:3680`) queues *additional* commands, so one
  command that touches many keys can emit several `DEL`s (or `SREM`s).

Both feed a per-execution-unit buffer that `propagatePendingCommands` (`server.c:3746`)
flushes to the AOF and replication stream when the unit finishes.

### Execution units — why propagation is deferred, not immediate

`call` can **recurse**. A Lua script, a module `RM_Call`, or `EXEC` re-enters `call` for each
inner command. Valkey wraps the whole outermost operation in an *execution unit* and flushes
propagation only when that unit finishes — `postExecutionUnitOperations` (`server.c:3799`).
The payoff is atomicity on the replica: everything a script does propagates as one block,
wrapped in `MULTI`/`EXEC`, so a replica can never observe a script half-applied. The same
batching is why keyspace notifications and client-side-cache invalidations are flushed at
unit end rather than mid-command.

If you have ever wondered how a replica stays byte-identical to its primary despite random,
time-dependent, and scripted commands — this is the entire answer.

## Where the command table lives

Commands are declared as JSON under `src/commands/*.json` and code-generated into
`commands.def` (via `utils/generate-command-code.py`, run by `make`). Don't hand-edit
`commands.def`; edit the JSON and regenerate. Each `struct serverCommand` (in `server.h`)
carries the proc pointer, arity, flags (`write`, `readonly`, `denyoom`, `admin`, ...), and —
importantly — **key specs**: which argv positions are keys. Those key specs are exactly what
ACL (check 4) and cluster redirection (check 5) consult to know which arguments are keys. One
declaration, consumed by multiple gatekeeping stages.

## Worked example — one `SPOP`, end to end

`myset` holds `{apple, banana, cherry}`. A client sends `SPOP myset`. Follow it all the way
through; this one command exercises the gatekeeper, the execution wrapper, *and* the
propagation rewrite.

**1. Read & parse.** `readQueryFromClient` (`networking.c:4341`) reads the bytes
`*2\r\n$4\r\nSPOP\r\n$5\r\nmyset\r\n` into the query buffer; `processInputBuffer`
(`networking.c:4203`) parses them into `c->argv = ["SPOP","myset"]`, `c->argc = 2`.

**2. Gatekeeping** (`processCommand`, `server.c:4315`). `SPOP` exists, arity 2 is valid; the
client is authenticated; ACL permits `SPOP` on `myset`; in cluster mode we own `myset`'s
slot; we're under `maxmemory`; `SPOP` is a write and we're a primary accepting writes. Every
check passes, so `processCommand` calls `call(c, CMD_CALL_FULL)`.

**3. Execution** (`call` → `spopCommand`, `t_set.c:953`). `call` snapshots `server.dirty`,
enters the execution unit, and invokes the proc. `spopCommand`:
   - picks a *random* member — say **`banana`**;
   - removes it from the set;
   - `addReplyBulk(c, banana)` — appends `$6\r\nbanana\r\n` to the client's **reply buffer**
     (not the socket — chapter 01);
   - fires the `spop` keyspace notification;
   - increments `server.dirty`.

**4. The propagation rewrite — the crux.** `banana` was chosen at random. If a replica
replayed the literal `SPOP myset`, it might pop `cherry` instead and diverge forever. So
`spopCommand` rewrites its own command vector (`t_set.c` ~972):

```c
rewriteClientCommandVector(c, 3, shared.srem, c->argv[1], ele);
//  c->argv is now:  ["SREM", "myset", "banana"]
```

The client still gets `banana` back — but what will be written to the AOF and sent to
replicas is the deterministic `SREM myset banana`. (If that pop emptied the set, the command
also emits a `DEL` and fires a `del` notification.)

**5. Propagate & reset.** `commandProcessed` (`networking.c:3879`) closes the execution unit;
`propagatePendingCommands` (`server.c:3746`) flushes `SREM myset banana` to the AOF buffer and
the replication backlog. `resetClient` (`networking.c:3365`) frees `argv` and clears
per-command state.

**6. Reply reaches the wire — next.** The `$6\r\nbanana\r\n` reply is still only in the
buffer. At the top of the next loop iteration, `beforeSleep`'s `handleClientsWithPendingWrites`
`write(2)`s it to the socket (chapter 01). The client sees `banana`; every replica sees
`SREM myset banana`; both databases now agree that `myset = {apple, cherry}`.

That single command is the whole chapter in miniature: gate, execute into a buffer, rewrite
for determinism, propagate at unit end, flush the reply later.

## Try it yourself

Add one line at the top of `call()` (`server.c:3875`):

```c
serverLog(LL_WARNING, "call %s depth=%d", c->cmd->fullname, server.execution_nesting);
```

Rebuild and run three things: a plain `GET`, a `MULTI/SET/SET/EXEC`, and
`EVAL "redis.call('set',KEYS[1],'x'); redis.call('incr',KEYS[2])" 2 a b`. Watch the nesting.
The plain command logs once at depth 0. `EXEC` logs the wrapper, then each queued command.
The script logs once for `EVAL`, then once per inner `redis.call`. Seeing how many times
`call()` re-enters for a single client request — and knowing they all flush propagation
together at unit end — teaches the execution-unit model faster than any amount of static
reading. Then run `MONITOR` in another client and watch a real `SPOP`: you'll see the server
report `SREM`, exactly as the worked example predicts.

## Read next

Chapter 03 — the `client` struct those `addReply` calls were filling, and how the reply
buffer actually drains to a socket.
