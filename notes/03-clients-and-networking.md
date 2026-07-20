# 03 — Clients & Networking

Chapter 02 ended with a reply sitting in a buffer, unsent. This chapter is about the thing
that owns that buffer — the `client` struct — and the two-stage machinery that eventually
gets those bytes onto a socket. The surprise that organizes everything here is how *much*
Valkey models as a client: not just the connection on the other end of your `valkey-cli`,
but replicas, the AOF, Lua scripts, and modules too. Learn the `client` struct and the
reply path once, and you've simultaneously learned how replication streams data, how the
AOF gets fed, and why a slow consumer can get itself disconnected.

`networking.c` is about 6,700 lines, but roughly 70% of it is `addReply*()` variants — one
per RESP type and convenience shape. Skip those on a first read; the structural code is maybe
1,500 lines, and it's what follows.

## The `client` struct is the universe

It's defined in `server.h` (the big struct around `server.h:1300`), it's enormous, and that
sprawl is the point: **almost everything in Valkey is modeled as a client.**

- A normal connection is a client.
- A **replica** is a client — the primary literally writes the replication stream into the
  replica-client's reply buffer, using the exact same `addReply` machinery a `GET` uses.
- The **AOF** is fed through a fake client.
- **Lua scripts** and **modules** execute through fake clients.
- The **cluster bus** is the one exception — it has its own link type (`clusterLink`,
  chapter 08), not a client.

Once this clicks, a whole class of "why is it built this way?" questions dissolves. Why does
replication reuse the reply-buffer code? Because a replica is just a client you never stop
writing to. A fake client is marked by `conn == NULL` — no real socket underneath.

The fields worth locating on a first read:

- **`querybuf`** — accumulates inbound bytes off the socket. `argv` / `argc` / `cmd` hold the
  parsed command once `processInputBuffer` has run.
- **`buf` + `bufpos`** — the fixed **static reply buffer** (16 KB, allocated per client). **`reply`**
  — the **overflow reply list**, used only when `buf` fills. **`reply_bytes`** — its total size.
- **`flag`** — a `struct ClientFlags` bitfield (not a plain int). Grep `c->flag.` and you're
  reading the client state machine: `multi`, `blocked`, `readonly`, `close_asap`,
  `pending_write`, `executing_command`, and so on.
- **`conn`** — the connection abstraction (TCP / TLS / Unix socket). `NULL` for a fake client.
- **replication-state fields** — `replstate`, `psync_initial_offset`, etc., meaningful when
  this client *is* a replica (chapter 07).

## Reading a request

`readQueryFromClient` (`networking.c:4341`) is the socket-readable handler — the top of
iteration B from chapter 01. With I/O threads enabled it may not run on the main thread: the
read and the parse can be offloaded (`trySendReadToIOThreads`, `io_threads.c:501`), but
command *execution* always returns to the main thread. The offload buys you parallel
`recv()` and RESP parsing without ever making two commands run at once.

Parsing happens in `processInputBuffer` (`networking.c:4203`), which loops over the query
buffer dispatching to one of two parsers:

- the **inline** parser — telnet-style `PING\r\n`, `PROTO_REQ_INLINE`, for humans and simple
  health checks; and
- the **multibulk** parser — real RESP, `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`,
  `PROTO_REQ_MULTIBULK`, which every client library uses.

Each fully parsed command populates `c->argv` / `c->argc` and calls
`processCommandAndResetClient` (`networking.c:3932`) → `processCommand` (chapter 02).

**Pipelining falls out for free.** If a client sent five commands back-to-back and all five
are sitting in the query buffer, the parse loop simply runs five times before returning to
the event loop — five commands, one socket read, one eventual write. And a very large
argument (≥ `PROTO_MBULK_BIG_ARG` = 32 KB, `server.h:214`) is special-cased: a big `SET`
value is read straight into its final object rather than copied through the general query
buffer, avoiding a needless gigabyte-sized memcpy on large payloads.

## Writing a reply — the two-stage system

This is the mechanism chapter 01 kept pointing at, and understanding the split explains a lot
of Valkey's memory and latency behavior.

**Stage 1 — during command execution.** `addReply()` (`networking.c:787`) and its many
siblings do **not** touch the socket. Each one first calls `prepareClientToWrite`
(`networking.c:447`) — the gate deciding *whether* this client should be written to at all (a
fake AOF client shouldn't; a replica in certain states shouldn't) and, on the first reply of
the iteration, registering the client via `putClientInPendingWriteQueue` (`networking.c:406`).
Then the bytes land in one of two places (`_addReplyToBufferOrList`, `networking.c:719`):

1. **`c->buf`** — the 16 KB static buffer (`PROTO_REPLY_CHUNK_BYTES`, `server.h:212`) allocated
   per client at `createClient` (`networking.c:297`). This is the fast path: no allocation, no
   list, just a memcpy and a bump of `bufpos`.
2. **`c->reply`** — a linked list of 16 KB blocks (`_addReplyProtoToList`, `networking.c:700`),
   used only once `c->buf` is full.

That `buf → reply` transition is the exact boundary between a "cheap reply" and a "reply that
costs allocations." It's why a `KEYS *` or a huge `LRANGE` shows up in memory and latency
profiles the way it does — big responses spill out of the static buffer into an allocated
list.

**Stage 2 — in `beforeSleep`.** `handleClientsWithPendingWrites` (`networking.c:3318`) walks
the pending-write list and calls `writeToClient` (`networking.c:3102`), which performs the
real `write(2)`. If the socket would block (its send buffer is full), it installs a writable
handler and finishes draining on a later loop iteration. `_postWriteToClient`
(`networking.c:2988`) does the after-write bookkeeping and can close a client flagged
`close_after_reply`.

So the rule chapter 01 asserted is enforced right here: **commands never write to sockets.**
They fill buffers; the event loop drains them. This is what lets one loop iteration batch the
replies of many pipelined commands into a single syscall.

## Client lifecycle

- **`createClient`** (`networking.c:285`) — on accept, or synthesized for a fake client
  (`conn == NULL`). Allocates the 16 KB static reply buffer up front.
- **`resetClient`** (`networking.c:3365`) — run between commands on the same connection:
  frees `argv`, clears per-command state. Called after every command completes.
- **`beforeNextClient`** (`networking.c:2328`) — per-client cleanup between clients in the
  processing loop.
- **Freeing is two-path**, and this is the subtle part:
  - `freeClient` (`networking.c:2116`) — synchronous teardown.
  - `freeClientAsync` (`networking.c:2247`) — sets `close_asap` and pushes the client onto
    `clients_to_close`; `freeClientsInAsyncFreeQueue` (`networking.c:2381`) drains that queue
    from `beforeSleep`. **Why async exists:** you routinely discover a client must die while
    standing *inside* code that's iterating over it — a failed `write`, a protocol error
    mid-parse. Freeing it synchronously would free the ground out from under the caller, so
    the teardown is deferred to the end of the loop iteration, where nothing is holding a
    pointer to it.

## Output-buffer limits — the safety valve

A slow client, or a replica that can't keep up, makes `c->reply` grow without bound: the
server keeps producing bytes (published messages, the replication stream) faster than the
consumer reads them. The `client-output-buffer-limit` config caps this with a hard limit
(disconnect immediately) and a soft limit (disconnect if exceeded for N seconds), enforced in
`beforeSleep` via `evictClients` (`server.c:2006`).

This is the machinery behind the classic replica resync loop: a replica falls behind → its
output buffer on the primary exceeds the limit → the primary disconnects it → the replica
reconnects and triggers a **full resync** → which is even more load → repeat. Dual-channel
replication (chapter 07) exists partly to break this loop.

## Worked example — one client, birth to reply to death

Follow a single connection through its whole life, watching which buffer each byte touches.

**1. Accept.** A TCP connection arrives on the listening socket. The accept handler calls
`createClient` (`networking.c:285`), which allocates the `client` struct and its 16 KB
`c->buf`, and registers `readQueryFromClient` as the fd's readable handler. The client's
reply buffers are empty; `bufpos == 0`, `c->reply` is an empty list.

**2. Request.** The client sends `GET foo`. The fd becomes readable; `readQueryFromClient`
(`networking.c:4341`) `recv()`s `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n` into `querybuf`;
`processInputBuffer` (`networking.c:4203`) parses it into `argv = ["GET","foo"]` and calls
into `processCommand` → `call` → `getCommand` (chapters 02, 04).

**3. Reply — Stage 1.** `getCommand` finds `foo = "bar"` and calls `addReply`
(`networking.c:787`). `prepareClientToWrite` runs `putClientInPendingWriteQueue`, adding this
client to the pending-write list (first reply this iteration). The 9 bytes `$3\r\nbar\r\n` are
memcpy'd into `c->buf` at offset 0; `bufpos` becomes 9. **No socket write has happened.** The
command returns; `resetClient` frees `argv`.

**4. Reply — Stage 2.** The loop finishes dispatching and reaches `beforeSleep`.
`handleClientsWithPendingWrites` (`networking.c:3318`) sees this client on the list and calls
`writeToClient` → one `write(2)` puts `$3\r\nbar\r\n` on the wire. `bufpos` resets to 0. The
client is off the pending-write list.

**5. Now make it misbehave.** Suppose instead the client had subscribed to a channel and then
stopped reading, while a publisher floods it. Each published message runs Stage 1 —
`addReply` — but the client never drains, so `write(2)` in Stage 2 keeps returning "would
block." Bytes pile up: first `c->buf` fills (16 KB), then every further message spills into
`c->reply`, growing the list block by 16 KB block. Its `omem` (visible in `CLIENT LIST`)
climbs. Once it crosses `client-output-buffer-limit pubsub`, `evictClients`
(`server.c:2006`) in `beforeSleep` frees the client — via the **async** path, because we
discovered the violation while walking the client list. On the next iteration
`freeClientsInAsyncFreeQueue` (`networking.c:2381`) actually tears it down.

Steps 3–5 are the whole chapter: replies are *staged* into per-client memory during
execution and *flushed* by the loop, and when a consumer can't keep up, that staged memory is
exactly what the safety valve measures and caps.

## Try it yourself

Reproduce step 5 directly. In one shell, flood a channel:
`while true; do valkey-cli publish ch "$(head -c 1000 </dev/zero | tr '\0' x)"; done`. In
another, subscribe but never read: `(printf 'SUBSCRIBE ch\r\n'; sleep 999) | nc localhost
6379`. Now watch `valkey-cli CLIENT LIST`: the `nc` client's `omem` climbs as its `c->reply`
list grows, and once it crosses `client-output-buffer-limit pubsub` the server disconnects
it. You've watched Stage-1 buffering with no Stage-2 drain, and the safety valve firing.
Contrast a fast reader, whose `omem` sits at 0 because `beforeSleep` empties `c->buf` every
iteration before it ever spills to the list.

## Read next

Chapter 04 — what's actually stored on the other side of that `lookupKey` in step 3: the
object model and the hash table that finds it.
