# 03 — Clients & Networking

`networking.c` is 6,700 lines, but ~70% of it is `addReply*()` variants — one per RESP
type and convenience shape. Skip those. The structural code is maybe 1,500 lines.

## The `client` struct is the universe

Defined in `server.h`. It is huge, and that's the point: **almost everything in Valkey
is modelled as a client.**

- A normal connection is a client.
- A **replica** is a client (the primary writes the replication stream to its reply buffer).
- The **AOF** is fed via a fake client.
- **Lua scripts** and **modules** execute through fake clients.
- The **cluster bus** is *not* — it has its own link type (`clusterLink`, note 08).

Once this clicks, a lot of the codebase stops being surprising: "why does replication
reuse the reply buffer machinery?" Because a replica is just a client you never stop
writing to.

Fields worth finding on first read: `querybuf`, `argv`/`argc`, `cmd`, `buf`/`bufpos`
(the fixed-size static reply buffer), `reply` (the overflow reply *list*), `flags`,
`conn`, and the replication-state fields (`replstate`, `psync_initial_offset`).

## Reading a request

`readQueryFromClient` (`networking.c:4341`) is the socket-readable handler.

With I/O threads enabled, this may not run on the main thread — the read and the parse
get offloaded (see `design-docs/io-threads.md`, and `trySendReadToIOThreads` at
`io_threads.c:501`). Command *execution* still happens on the main thread regardless.

Parsing: `processInputBuffer` (`networking.c:4203`) loops over the query buffer,
dispatching to either the **inline** parser (simple `PING\r\n` telnet-style) or the
**multibulk** parser (real RESP: `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`). Each fully parsed
command populates `c->argv`/`c->argc` and then calls `processCommandAndResetClient`
(`networking.c:3932`) → `processCommand` (note 02).

Pipelining falls out of this for free: if the query buffer holds five commands, the loop
just runs five times before returning to the event loop.

## Writing a reply

This is a **two-stage** system, and understanding the split explains a lot of the
performance behavior.

**Stage 1 — during command execution.** `addReply()` (`networking.c:787`) and its many
siblings do *not* touch the socket. They append bytes to:

1. `c->buf` — a fixed-size (16KB by default) static buffer on the client. Fast path.
2. `c->reply` — a linked list of larger blocks, used once `c->buf` is full.

The first `addReply` on an otherwise-idle client calls `putClientInPendingWriteQueue`
(`networking.c:406`), which registers the client in the server's "has pending output"
list.

**Stage 2 — in `beforeSleep`.** `handleClientsWithPendingWrites`
(`networking.c:3318`) walks that list and calls `writeToClient` (`networking.c:3102`),
which does the actual `write(2)`. If the socket would block, it installs a writable
handler and finishes on a later loop iteration. `postWriteToClient`
(`networking.c:3054`) does the bookkeeping after a write completes.

So: **commands never write to sockets.** They fill buffers; the event loop drains them.
This is what lets one loop iteration batch replies for many pipelined commands into a
single syscall, and it's why `beforeSleep` (note 01) matters so much.

## Client lifecycle

- `createClient` (`networking.c:285`) — on accept, or synthesized for fake clients
  (fake clients pass `conn == NULL`).
- `resetClient` (`networking.c:3365`) — between commands on the same connection: frees
  `argv`, clears per-command state. Called after every command.
- `freeClient` — teardown. Note there is also an **async** free path, because you often
  cannot free a client while you're standing inside code that's iterating over it;
  Valkey defers it to the end of the loop iteration.

`resetClientIOState` (`networking.c:3415`) exists because I/O threads may have in-flight
work on a client, which must be reconciled before the client's state is reused.

## Output buffer limits

A slow client (or a replica that can't keep up) makes `c->reply` grow without bound. The
`client-output-buffer-limit` config kills clients that exceed hard/soft limits — this is
checked as the reply list grows. This is the mechanism behind the classic "replica
disconnected, resync loop" failure mode: replica falls behind → output buffer exceeds
limit → primary kills it → replica reconnects → full sync → repeat.

## Read next

Note 04 — what's actually stored on the other side of `lookupKey`.
