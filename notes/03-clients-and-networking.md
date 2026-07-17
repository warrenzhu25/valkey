# 03 — Clients & Networking

`networking.c` is 6,700 lines, but ~70% of it is `addReply*()` variants — one per RESP
type and convenience shape. Skip those. The structural code is maybe 1,500 lines.

## The `client` struct is the universe

Defined in `server.h` (the big struct around `server.h:1300`+). It is huge, and that's the
point: **almost everything in Valkey is modelled as a client.**

- A normal connection is a client.
- A **replica** is a client (the primary writes the replication stream to its reply buffer).
- The **AOF** is fed via a fake client.
- **Lua scripts** and **modules** execute through fake clients.
- The **cluster bus** is *not* — it has its own link type (`clusterLink`, note 08).

Once this clicks, a lot of the codebase stops being surprising: "why does replication
reuse the reply buffer machinery?" Because a replica is just a client you never stop
writing to.

Fields worth finding on first read:
- `querybuf` (`server.h:1302`) — accumulates inbound bytes; `argv`/`argc`, `cmd` — the parsed command.
- `buf` (`server.h:1332`) + `bufpos` — the **fixed static reply buffer**; `reply`
  (`server.h:1334`) — the **overflow reply list**; `reply_bytes` — its total size.
- `flag` (`server.h:1351`) — a `struct ClientFlags` bitfield (`server.h:1112`), not a plain
  int anymore; grep `c->flag.` to see the state machine (`multi`, `blocked`, `readonly`,
  `close_asap`, `pending_write`, `executing_command`, …).
- `conn` — the connection abstraction (socket/TLS/unix); **`conn == NULL` marks a fake client**.
- replication-state fields (`replstate`, `psync_initial_offset`) for when this client is a replica.

## Reading a request

`readQueryFromClient` (`networking.c:4341`) is the socket-readable handler.

With I/O threads enabled, this may not run on the main thread — the read and the parse
get offloaded (see `design-docs/io-threads.md`, and `trySendReadToIOThreads` at
`io_threads.c:501`). Command *execution* still happens on the main thread regardless.

Parsing: `processInputBuffer` (`networking.c:4203`) loops over the query buffer,
dispatching to either the **inline** parser (simple `PING\r\n` telnet-style,
`PROTO_REQ_INLINE`) or the **multibulk** parser (real RESP:
`*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`, `PROTO_REQ_MULTIBULK`). Each fully parsed command
populates `c->argv`/`c->argc` and then calls `processCommandAndResetClient`
(`networking.c:3932`) → `processCommand` (note 02).

Pipelining falls out of this for free: if the query buffer holds five commands, the loop
just runs five times before returning to the event loop. A large multibulk argument
(≥ `PROTO_MBULK_BIG_ARG`, 32KB, `server.h:214`) gets special-cased so a big `SET` value is
read straight into place rather than copied through the general query buffer.

## Writing a reply

This is a **two-stage** system, and understanding the split explains a lot of the
performance behavior.

**Stage 1 — during command execution.** `addReply()` (`networking.c:787`) and its many
siblings do *not* touch the socket. First they call `prepareClientToWrite`
(`networking.c:447`) — the gate that decides *whether* this client should be written to at
all (a fake AOF client shouldn't; a replica in some states shouldn't) and, on the first
reply of the iteration, registers the client via `putClientInPendingWriteQueue`
(`networking.c:406`). Then the bytes go to one of two places, via `_addReplyToBufferOrList`
(`networking.c:719`):

1. `c->buf` — a **16KB** (`PROTO_REPLY_CHUNK_BYTES`, `server.h:212`) static buffer allocated
   per client at `createClient` (`networking.c:297`). Fast path — no allocation, no list.
2. `c->reply` — a linked list of `PROTO_REPLY_CHUNK_BYTES` blocks, used once `c->buf` fills.
   `_addReplyProtoToList` (`networking.c:700`) appends here.

The buf→list transition is the boundary between "cheap reply" and "reply that costs
allocations", which is why big multi-bulk responses (a `KEYS *`, a huge `LRANGE`) show up
in memory and latency profiles the way they do.

**Stage 2 — in `beforeSleep`.** `handleClientsWithPendingWrites` (`networking.c:3318`)
walks the pending-write list and calls `writeToClient` (`networking.c:3102`), which does
the actual `write(2)`. If the socket would block, it installs a writable handler and
finishes on a later loop iteration. `postWriteToClient` (`networking.c:3054`) does the
bookkeeping after a write completes (and may close a client flagged `close_after_reply`).

So: **commands never write to sockets.** They fill buffers; the event loop drains them.
This is what lets one loop iteration batch replies for many pipelined commands into a
single syscall, and it's why `beforeSleep` (note 01) matters so much.

## Client lifecycle

- `createClient` (`networking.c:285`) — on accept, or synthesized for fake clients
  (fake clients pass `conn == NULL`). Allocates the 16KB static reply buffer.
- `resetClient` (`networking.c:3365`) — between commands on the same connection: frees
  `argv`, clears per-command state. Called after every command.
- `beforeNextClient` (`networking.c:2328`) — per-client cleanup run between clients in the
  processing loop.
- **Freeing is two-path**, and this is the subtle part:
  - `freeClient` (`networking.c:2116`) — synchronous teardown.
  - `freeClientAsync` (`networking.c:2247`) — sets `CLIENT_CLOSE_ASAP` and pushes the
    client onto `clients_to_close`; `freeClientsInAsyncFreeQueue` (`networking.c:2381`)
    drains it from `beforeSleep`. **Why async exists:** you frequently discover a client
    must die while standing *inside* code iterating over it (a failed write, a protocol
    error mid-parse). Freeing it synchronously would yank the ground out from under the
    caller, so the free is deferred to the end of the loop iteration.

`resetClientIOState` (`networking.c:3415`) exists because I/O threads may have in-flight
work on a client, which must be reconciled before the client's state is reused.

## Output buffer limits

A slow client (or a replica that can't keep up) makes `c->reply` grow without bound. The
`client-output-buffer-limit` config kills clients that exceed hard/soft limits — this is
checked as the reply list grows, and enforced in `beforeSleep` via `evictClients`
(`server.c:2006`). This is the mechanism behind the classic "replica disconnected, resync
loop" failure mode: replica falls behind → output buffer exceeds limit → primary kills
it → replica reconnects → full sync → repeat. (Dual-channel replication, note 07, exists
partly to break this loop.)

## Exercise

Run `valkey-cli` with `CLIENT NO-EVICT on`, then in another shell:
`valkey-cli debug sleep 0` won't help — instead subscribe and never read:
`(printf 'SUBSCRIBE ch\r\n'; sleep 999) | nc localhost 6379` while a publisher floods
`ch`. Watch `CLIENT LIST` — the subscriber's `omem` (output-buffer memory) climbs as its
`c->reply` list grows, and once it crosses `client-output-buffer-limit pubsub`, the server
kills it. You've just watched Stage-1 buffering with no Stage-2 drain, and the safety valve
firing. Contrast with a fast reader, whose `omem` stays at 0 because `beforeSleep` drains
`c->buf` every iteration before it ever spills to the list.

## Read next

Note 04 — what's actually stored on the other side of `lookupKey`.
