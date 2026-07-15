# 02 — Command Execution Path

Trace one `GET foo` from socket to reply. If you learn one path in this codebase, learn
this one — every feature hooks into it somewhere.

## The path

```
socket readable
  └─ readQueryFromClient              networking.c
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

... later, same loop iteration ...
beforeSleep  server.c:1854
  └─ flush reply buffers to sockets
```

## `processCommand` — the gatekeeper (`server.c:4315`)

This is the single most consequential function in the server. It does **not** execute
anything; it decides whether execution is allowed. In rough order it checks:

- Command lookup — does the command exist? Is the arity right?
- **Authentication** — is the client authed?
- **ACL** — may *this user* run *this command* on *these keys*?
- **Cluster redirection** — do we own the slot for these keys? If not, reply `-MOVED`
  or `-ASK` and stop. (See note 08; `getNodeByQuery` at `cluster.c:1048`.)
- **maxmemory** — if we're over the limit, try to evict; if we can't free enough and the
  command uses memory, reject with `-OOM`.
- **Persistence errors** — if the last background save failed and `stop-writes-on-bgsave-error`
  is set, reject writes.
- **Replica state** — a read-only replica rejects writes from normal clients.
- **Pub/Sub mode, MULTI queuing, blocked clients, loading state, busy script...**

If any check fails, it replies with an error and returns without calling `call()`. This
is why "why did my command get rejected?" is nearly always answered by reading
`processCommand` top to bottom.

**MULTI/EXEC note:** if the client is in a transaction, `processCommand` queues the
command instead of executing it. `EXEC` later calls `call()` on each queued command.

## `call` — the execution wrapper (`server.c:3875`)

By the time you're here, the command *is* going to run. `call()` is the wrapper that
makes execution observable and durable:

- Records start time, runs `c->cmd->proc(c)` (the actual command implementation, e.g.
  `getCommand` in `t_string.c`), records duration.
- Updates stats: calls, microseconds, errors; feeds the **slowlog** and **latency monitor**.
- Fires **keyspace notifications**.
- Accumulates what should be **propagated** to the AOF and replicas.
- Handles **command tracking / client-side caching** invalidation.
- Recurses: a Lua script or module calling back into the server re-enters `call()`.

### Propagation is not "log the command you ran"

This trips everyone up. Valkey often propagates something *different* from what the
client sent, because commands must be **deterministic** on replay. `SPOP` propagates as
`SREM` with the specific member it popped. `EXPIRE` propagates as `PEXPIREAT` with an
absolute timestamp. `INCRBYFLOAT` propagates as `SET`.

The command implementation signals this by calling `rewriteClientCommandVector()` or by
adding to the propagation buffer directly (`alsoPropagate`). `call()` and
`commandProcessed` then push the *rewritten* form to the AOF and replicas.

If you have ever wondered how a replica stays byte-identical to its primary despite
random and time-dependent commands: this is the mechanism.

## Where the command table lives

Commands are defined in JSON under `src/commands/*.json` and code-generated into
`commands.def`. If you add a command, you edit the JSON and regenerate — don't hand-edit
`commands.def`. The `struct serverCommand` (in `server.h`) carries the proc pointer,
arity, flags (`write`, `readonly`, `denyoom`, `admin`...), and key specs (which argv
positions are keys — this is what ACL and cluster redirection consult).

## Exercise

Set a breakpoint or add a `serverLog()` in `call()` printing `c->cmd->fullname`, then run
a `MULTI`/`EXEC` and a Lua script. Watching how many times `call()` re-enters for one
client request tells you more about the architecture than reading it does.
