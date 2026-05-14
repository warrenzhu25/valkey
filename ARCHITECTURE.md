# Valkey Architecture

This document is a code-referenced introduction to the Valkey server architecture for engineers who want to understand runtime behavior, subsystem boundaries, and where to start reading.

It is based on the current repository layout and source structure under `src/`, `src/unit/`, and `tests/`.

## 1. High-Level Overview

At runtime, Valkey is an event-driven in-memory data server centered around a single global process state object, `server`, declared in [`src/server.c`](src/server.c) and defined by `struct valkeyServer` in [`src/server.h`](src/server.h).

The server is organized around a small number of core loops and state transitions:

- Process bootstrap and configuration loading
- Event loop initialization and listener setup
- Client accept / read / parse / dispatch / reply
- Background maintenance via cron-style timers
- Persistence, replication, and cluster coordination

The main execution path starts in `main()` in [`src/server.c`](src/server.c), initializes configuration and subsystems, loads persisted state if needed, and then enters the event loop with `aeMain(server.el)`.

In plain terms:

```text
startup
  -> initialize global server state
  -> load config
  -> create listeners
  -> load data from disk
  -> enter event loop

event loop
  -> accept clients
  -> read protocol input
  -> parse commands
  -> run command implementation
  -> queue replies
  -> flush replies
  -> run periodic maintenance
```

Although Valkey uses I/O threads for some read/write and parsing work, command semantics and dataset mutation remain centered in the main runtime flow. The code is best understood as a single-threaded command engine with selective offloading around the edges.

## 2. Repository Map

### Top-Level Areas

- `src/`: main server implementation
- `src/unit/`: GoogleTest unit tests for low-level components
- `tests/`: Tcl integration and behavioral tests
- `design-docs/`: design notes and supporting documents
- `deps/`: vendored third-party dependencies

### Important `src/` Areas

- [`src/server.c`](src/server.c): bootstrap, lifecycle, command dispatch, cron, shutdown
- [`src/server.h`](src/server.h): central data structures and shared declarations
- [`src/networking.c`](src/networking.c): client lifecycle, protocol I/O, replies
- [`src/connection.c`](src/connection.c): transport abstraction registration
- [`src/ae.c`](src/ae.c): event loop
- [`src/config.c`](src/config.c): config parsing and `CONFIG` command implementation
- [`src/db.c`](src/db.c): key lookup, mutation, expiry interaction, DB-level operations
- [`src/object.c`](src/object.c): object representation and allocation strategy
- [`src/rdb.c`](src/rdb.c): snapshot loading/saving
- [`src/aof.c`](src/aof.c): append-only persistence
- [`src/replication.c`](src/replication.c): primary/replica synchronization and propagation
- [`src/cluster.c`](src/cluster.c): cluster routing and state management
- [`src/module.c`](src/module.c): loadable module system
- [`src/eval.c`](src/eval.c), [`src/functions.c`](src/functions.c), [`src/scripting_engine.c`](src/scripting_engine.c): scripting and functions
- [`src/t_string.c`](src/t_string.c), [`src/t_list.c`](src/t_list.c), [`src/t_set.c`](src/t_set.c), [`src/t_hash.c`](src/t_hash.c), [`src/t_zset.c`](src/t_zset.c), [`src/t_stream.c`](src/t_stream.c): type-specific command implementations

### Tests

- `src/unit/*.cpp`: focused unit coverage for data structures and utility layers
- `tests/unit/*.tcl`: command behavior and feature-level tests
- `tests/integration/*.tcl`: replication, persistence, failover, compatibility flows
- `tests/unit/moduleapi/*.tcl`: module API behaviors

## 3. Major Component Map

This section identifies the subsystems that matter most when onboarding.

### 3.1 Bootstrap and Process Lifecycle

- Purpose: bring up the server process, initialize global state, load configuration, start runtime subsystems
- Primary files:
  - [`src/server.c`](src/server.c)
  - [`src/config.c`](src/config.c)
- Key functions:
  - `main()`
  - `initServerConfig()`
  - `loadServerConfig()`
  - `initServer()`
  - `InitServerLast()`
  - `loadDataFromDisk()`
- Main dependencies:
  - ACL, module system, connection types, event loop, persistence, cluster

### 3.2 Configuration System

- Purpose: load config from files/stdin/CLI and support runtime config accessors
- Primary files:
  - [`src/config.c`](src/config.c)
- Key functions:
  - `loadServerConfigFromString()`
  - `loadServerConfig()`
- Notable behavior:
  - Handles `include`, `rename-command`, `user`, `loadmodule`, module config namespaces, and sentinel directives

### 3.3 Event Loop

- Purpose: multiplex file and time events
- Primary files:
  - [`src/ae.c`](src/ae.c)
  - [`src/ae_epoll.c`](src/ae_epoll.c), [`src/ae_kqueue.c`](src/ae_kqueue.c), [`src/ae_select.c`](src/ae_select.c)
- Key functions:
  - `aeCreateEventLoop()`
  - `aeProcessEvents()`
  - `aeMain()`
- Interactions:
  - file events drive client/network work
  - time events drive `serverCron()` and `clientsTimeProc()`
  - `beforeSleep()` and `afterSleep()` hooks integrate server-specific maintenance

### 3.4 Connection and Listener Abstraction

- Purpose: abstract TCP, Unix, TLS, and RDMA transports behind one interface
- Primary files:
  - [`src/connection.c`](src/connection.c)
  - [`src/socket.c`](src/socket.c)
  - [`src/unix.c`](src/unix.c)
  - [`src/tls.c`](src/tls.c)
  - [`src/rdma.c`](src/rdma.c)
- Key functions:
  - `connTypeInitialize()`
  - `connectionByType()`
  - `initListeners()`

### 3.5 Client, Protocol, and Networking

- Purpose: accept connections, read protocol input, parse commands, queue and flush replies
- Primary files:
  - [`src/networking.c`](src/networking.c)
  - [`src/server.h`](src/server.h)
- Key structs:
  - `client`
  - `ClientFlags`
  - `parsedCommand`
  - `clientReplyBlock`
- Key functions:
  - `acceptCommonHandler()`
  - `createClient()`
  - `readQueryFromClient()`
  - `processInputBuffer()`
  - `processCommandAndResetClient()`
  - `writeToClient()`
  - `sendReplyToClient()`

### 3.6 Command Table and Dispatch

- Purpose: represent command metadata and dispatch command procedures
- Primary files:
  - [`src/server.c`](src/server.c)
  - [`src/commands.h`](src/commands.h)
  - generated command table referenced as `serverCommandTable`
- Key structs:
  - `serverCommand`
  - `serverCommandArg`
- Key functions:
  - `populateCommandTable()`
  - `lookupCommandLogic()`
  - `processCommand()`
  - `call()`

### 3.7 In-Memory Data Model and DB Layer

- Purpose: store values, resolve keys, manage expiry and mutation side effects
- Primary files:
  - [`src/db.c`](src/db.c)
  - [`src/object.c`](src/object.c)
  - [`src/server.h`](src/server.h)
- Key structs:
  - `serverDb`
  - `serverObject` / `robj`
- Key functions:
  - `lookupKeyRead()`
  - `lookupKeyWrite()`
  - `dbAdd()`
  - `setKey()`
  - `dbDelete()`
  - `signalModifiedKey()`

### 3.8 Core Data Structures

- Purpose: provide memory-efficient internal containers
- Primary files:
  - [`src/kvstore.c`](src/kvstore.c)
  - [`src/hashtable.c`](src/hashtable.c)
  - [`src/rax.c`](src/rax.c)
  - [`src/quicklist.c`](src/quicklist.c)
  - [`src/listpack.c`](src/listpack.c)
  - [`src/intset.c`](src/intset.c)
  - [`src/vset.c`](src/vset.c)
- Notes:
  - These are reused across DB storage, replication indexes, pubsub maps, command metadata, and type implementations.

### 3.9 Persistence

- Purpose: durable state via RDB snapshots and AOF streams
- Primary files:
  - [`src/rdb.c`](src/rdb.c)
  - [`src/aof.c`](src/aof.c)
  - [`src/rio.c`](src/rio.c)
- Key functions:
  - `rdbLoad()`
  - `rdbLoadRioWithLoadingCtx()`
  - `aofLoadManifestFromDisk()`
  - `loadAppendOnlyFiles()`
  - `startAppendOnly()`

### 3.10 Replication

- Purpose: maintain primary/replica synchronization and command propagation
- Primary files:
  - [`src/replication.c`](src/replication.c)
  - [`src/server.c`](src/server.c)
- Key functions:
  - `replicationFeedReplicas()`
  - `feedReplicationBacklog()`
  - `createReplicationBacklog()`
- Key state:
  - `server.primary_*`
  - `server.repl_*`
  - `client.repl_data`

### 3.11 Cluster

- Purpose: slot-aware routing, redirection, cluster membership/state
- Primary files:
  - [`src/cluster.c`](src/cluster.c)
  - [`src/cluster_legacy.c`](src/cluster_legacy.c)
  - [`src/cluster_migrateslots.c`](src/cluster_migrateslots.c)
- Key functions:
  - `clusterInit()`
  - `clusterInitLast()`
  - `getNodeByQuery()`
  - `clusterRedirectClient()`
  - `clusterCron()`

### 3.12 Modules and Extensibility

- Purpose: load external features into the server and expose the module API
- Primary files:
  - [`src/module.c`](src/module.c)
  - [`src/module.h`](src/module.h)
  - [`src/valkeymodule.h`](src/valkeymodule.h)
- Key functions:
  - `moduleInitModulesSystem()`
  - `moduleInitModulesSystemLast()`
  - `moduleLoadFromQueue()`
  - `ValkeyModuleCommandDispatcher()`

### 3.13 Scripting and Functions

- Purpose: run EVAL-style scripts and server-side functions through pluggable scripting engines
- Primary files:
  - [`src/eval.c`](src/eval.c)
  - [`src/functions.c`](src/functions.c)
  - [`src/script.c`](src/script.c)
  - [`src/scripting_engine.c`](src/scripting_engine.c)
- Key functions:
  - `scriptingEngineManagerInit()`
  - `functionsInit()`
  - `evalInit()`

## 4. Startup and Runtime Flow

### 4.1 Entry Point

The binary entry point is `main()` in [`src/server.c`](src/server.c).

Key early steps:

1. Initialize allocator and random/hash seeds
2. Detect sentinel mode
3. Initialize default config with `initServerConfig()`
4. Initialize ACL, module system, and connection types
5. Parse config sources with `loadServerConfig()`

### 4.2 Server Initialization

`initServer()` performs the bulk of runtime state setup:

- installs signal handlers
- creates core lists and indexes for clients and replicas
- initializes databases
- creates the event loop with `aeCreateEventLoop()`
- registers `serverCron` and `clientsTimeProc`
- installs module wakeup pipe readable event
- installs `beforeSleep()` and `afterSleep()`
- initializes scripting manager, commandlog, latency monitor, shared query buffer, functions, and eval

### 4.3 Listener Setup

`initListeners()` maps config into transport listeners:

- TCP listener when `server.port != 0`
- TLS listener when configured
- Unix socket listener when `server.unixsocket != NULL`
- RDMA listener when configured

Each listener is opened through the connection-type abstraction and gets an accept handler registered in the event loop.

### 4.4 Late Initialization

`InitServerLast()` runs after module loading:

- `bioInit()`
- `initIOThreads(1)`
- allocator background thread configuration
- initial memory overhead baseline capture

This sequencing exists because some thread creation must happen after module loading to avoid loader/TLS races.

### 4.5 Persistence Load

If not in sentinel mode:

- `aofLoadManifestFromDisk()`
- `loadDataFromDisk()`
- `aofOpenIfNeededOnServerStart()`

`loadDataFromDisk()` chooses between:

- `loadAppendOnlyFiles(server.aof_manifest)` if AOF is enabled
- `rdbLoad(server.rdb_filename, ...)` otherwise

### 4.6 Event Loop Start

Finally the server enters:

```c
aeMain(server.el);
```

in [`src/server.c`](src/server.c).

## 5. Deep Dive Per Subsystem

### 5.1 Event Loop

The event loop implementation is in [`src/ae.c`](src/ae.c). It abstracts the polling backend through backend-specific files included at compile time.

Important control points:

- `aeCreateEventLoop()`: allocates file/time event tables
- `aeProcessEvents()`: runs before-sleep hook, polls, dispatches file events, dispatches time events
- `aeMain()`: loops until `eventLoop->stop`

Valkey-specific behavior is attached through:

- `beforeSleep()` in [`src/server.c`](src/server.c)
- `afterSleep()` in [`src/server.c`](src/server.c)

Those hooks are where the generic poll loop becomes a database server runtime.

What to read first:

- `aeProcessEvents()`
- `aeMain()`
- `beforeSleep()`
- `afterSleep()`

### 5.2 Client and Networking Layer

The `client` struct in [`src/server.h`](src/server.h) is one of the most important objects in the codebase. It combines:

- connection handle
- input buffer state
- parsed/current command state
- selected DB
- output buffer state
- pubsub / replication / module / transaction / blocking state

Client creation starts in `acceptCommonHandler()` in [`src/networking.c`](src/networking.c), which:

1. validates accept state
2. enforces `maxclients`
3. creates a `client` via `createClient()`
4. initiates transport-level accept handling

`createClient()` sets the read handler to `readQueryFromClient()` for connected clients, allocates the reply buffer, assigns a unique ID, initializes auth state, and links the client into global indexes.

Input flow:

```text
readQueryFromClient
  -> readToQueryBuf
  -> processInputBuffer
      -> consumeCommandQueue or parseInputBuffer
      -> prepareCommandQueue
      -> prefetchCommandQueueKeys
      -> handleParseResults
      -> processCommandAndResetClient
```

Reply flow:

- command implementations call `addReply*()`
- replies land in `client->buf` and/or `client->reply`
- pending write queue is used first
- full async write handlers are installed only when needed

Non-obvious detail:

- reply construction supports copy-avoidance for large bulk strings, using encoded reply buffers and `bulkStrRef` metadata in [`src/networking.c`](src/networking.c)

What to read first:

- `createClient()`
- `readQueryFromClient()`
- `processInputBuffer()`
- `writeToClient()`

### 5.3 Command Table and Dispatch

Command metadata is represented by `struct serverCommand` in [`src/server.h`](src/server.h). It includes:

- command name(s)
- flags
- implementation function
- key specs
- docs/summary/history metadata
- subcommands

`populateCommandTable()` in [`src/server.c`](src/server.c) loads commands from `serverCommandTable`, which is generated from command definitions.

Command lookup:

- `lookupCommandLogic()`
- `lookupCommand()`
- `lookupCommandBySdsLogic()`

Dispatch is intentionally split:

- `processCommand()` handles admission and routing logic
- `call()` executes the command proc and post-execution bookkeeping

`processCommand()` is where the server rejects or defers commands for reasons like:

- auth required
- unknown command
- wrong arity
- protected command restrictions
- ACL denial
- cluster redirection
- redirect in standalone replica mode
- OOM
- disk errors
- replica write restrictions
- stale replica restrictions
- loading restrictions
- busy script/module conditions
- client pause

`call()` then handles:

- actual `c->cmd->proc(c)` invocation
- latency accounting
- commandlog and monitor feed
- propagation decisions
- tracking invalidations
- after-command maintenance

This separation is one of the most important architectural boundaries in the server.

### 5.4 Object Model

Valkey’s value model revolves around `robj`, defined as `struct serverObject` in [`src/server.h`](src/server.h).

Important fields:

- `type`
- `encoding`
- `lru`
- `refcount`
- embedded key/expire/value flags

The implementation in [`src/object.c`](src/object.c) is heavily optimized around allocation shape.

Important ideas:

- small strings may be EMBSTR and stored together with the object header
- key name and expiry may also be embedded into the same allocation
- shared objects use special refcounts
- object touch behavior integrates with LRU/LFU eviction logic

This is a performance-critical layer and worth reading carefully if you are touching memory behavior.

### 5.5 DB Layer and Keyspace Operations

The DB layer in [`src/db.c`](src/db.c) mediates almost all access to keys.

Key responsibilities:

- key lookup
- expiry interaction
- hit/miss accounting
- keyspace notifications
- mutation bookkeeping
- blocked-client readiness signaling

`lookupKey()` is especially central. It may:

1. find the value in the underlying `kvstore`
2. expire the key if needed
3. update LRU/LFU
4. increment hit/miss stats
5. emit `keymiss` notifications

Mutation helpers like `dbAdd()`, `setKey()`, and `dbDelete()` are responsible for storing values and triggering side effects like:

- `signalModifiedKey()`
- `notifyKeyspaceEvent()`
- ready-key signaling for blocked operations

Cluster mode affects this layer directly through slot-aware `kvstore` indexing.

### 5.6 Persistence

Persistence is split into snapshot and append-only modes.

#### RDB

Primary file: [`src/rdb.c`](src/rdb.c)

Key responsibilities:

- parse RDB opcodes and metadata
- create objects from persisted values
- populate DBs and function libraries during load
- support replication-specific load contexts

Important entry points:

- `rdbLoad()`
- `rdbLoadRio()`
- `rdbLoadRioWithLoadingCtx()`
- `rdbLoadObject()`

#### AOF

Primary file: [`src/aof.c`](src/aof.c)

Key responsibilities:

- load manifest and append-only file segments
- rewrite / rotate AOF state
- open AOF at startup when needed

Important entry points:

- `aofLoadManifestFromDisk()`
- `loadAppendOnlyFiles()`
- `startAppendOnly()`

### 5.7 Replication

Replication is a core runtime concern, not a peripheral feature.

Key state exists both in:

- global `server` fields in [`src/server.h`](src/server.h)
- per-client `ClientReplicationData` in [`src/server.h`](src/server.h)

Important responsibilities:

- primary-side command fanout
- replication backlog maintenance
- replica handshake and sync
- partial/full resync state transitions
- dual-channel replication flows

Important functions:

- `replicationFeedReplicas()`
- `feedReplicationBacklog()`
- replica handshake and transfer code in [`src/replication.c`](src/replication.c)

Notable design detail:

- replica output uses shared `replBufBlock` chains rather than isolated per-replica copies, reducing duplication and making backlog/replica sharing explicit

#### Primary-Side Replication Path

On the primary, replication is fed from the same command execution flow that drives AOF propagation.

The important boundary is in `call()` in [`src/server.c`](src/server.c):

- command implementations mutate state
- `call()` determines whether propagation is needed
- propagated operations are funneled into the replication path

At a high level:

```text
command execution
  -> call()
  -> alsoPropagate(...)
  -> replicationFeedReplicas(...)
  -> feedReplicationBacklog(...)
  -> replica output buffers / shared repl buffer blocks
```

Important entry points on the primary side:

- `createReplicationBacklog()` in [`src/replication.c`](src/replication.c)
- `replicationFeedReplicas()` in [`src/replication.c`](src/replication.c)
- `feedReplicationBacklog()` in [`src/replication.c`](src/replication.c)
- `getPsyncInitialOffset()` in [`src/replication.c`](src/replication.c)

The replication backlog is not just a byte buffer. It is managed as a linked list of shared `replBufBlock` allocations, indexed periodically for PSYNC lookup efficiency. Replica clients and the backlog both reference the same block chain, and trimming is reference-count aware.

This design matters because:

- partial resync is served from the backlog
- replicas do not need independent full copies of every pending propagation byte
- output memory and backlog memory are intentionally coupled

#### Replica-Side Synchronization State Machine

Replica synchronization is implemented as an explicit state machine in [`src/replication.c`](src/replication.c).

The main driver is:

- `syncWithPrimary()` in [`src/replication.c`](src/replication.c)

It delegates to state-specific handlers such as:

- `syncWithPrimaryHandleConnectingState()`
- `syncWithPrimaryHandleReceivePingReplyState()`
- `syncWithPrimaryHandleSendHandshakeState()`
- `syncWithPrimaryHandleReceiveAuthReplyState()`
- `syncWithPrimaryHandleReceivePortReplyState()`
- `syncWithPrimaryHandleReceiveIPReplyState()`
- `syncWithPrimaryHandleReceiveCapaReplyState()`
- `syncWithPrimaryHandleReceiveVersionReplyState()`
- `syncWithPrimaryHandleReceiveNodeIDReplyState()`
- `syncWithPrimaryHandleSendPsyncState()`

This is one of the clearest places in the code where distributed protocol sequencing is made explicit rather than hidden behind callbacks.

The rough handshake flow is:

```text
replica cron / reconnect logic
  -> connect to primary
  -> send PING / REPLCONF / AUTH-related handshake
  -> send PSYNC
  -> primary accepts partial resync
     or
     primary requests full resync
  -> load transferred RDB
  -> transition to streaming command propagation
```

Important details:

- `primaryTryPartialResynchronization()` handles the primary-side PSYNC decision path
- `replicationCachePrimaryUsingMyself()` preserves enough state to improve later partial resync chances after restart/load scenarios
- `replicationSendAck()` keeps the primary informed about replica progress and health

#### Full Resync and RDB Transfer

When partial resync is not possible, replication falls back to a full sync.

Key functions involved:

- `replicationSetupReplicaForFullResync()` on the primary side
- `sendBulkToReplica()` in [`src/replication.c`](src/replication.c)
- RDB loading paths reused through `rdbLoad*()` in [`src/rdb.c`](src/rdb.c)

The important architectural point is that full sync is not a separate persistence system. Replication reuses the RDB machinery for snapshot transfer and then switches into command stream continuation.

This means that RDB loading behavior, async loading, function-library loading, and replication correctness are tightly coupled.

#### Ongoing Maintenance

Replication maintenance is driven periodically by `replicationCron()` in [`src/replication.c`](src/replication.c), which is scheduled from `serverCron()` in [`src/server.c`](src/server.c).

It is responsible for tasks like:

- reconnect attempts
- periodic ACK handling
- keepalive propagation
- replica timeout handling
- progress transitions after sync

What to read first:

- `createReplicationBacklog()`
- `replicationFeedReplicas()`
- `primaryTryPartialResynchronization()`
- `syncWithPrimary()`
- `replicationCron()`

Common contributor pitfalls:

- treating replication as a separate output channel instead of part of command execution
- missing that backlog memory is shared with replica output state
- changing persistence load behavior without considering replication load paths
- assuming replica command application is just “normal client command execution” without the `mustObeyClient()` and replicated-client special cases in [`src/server.c`](src/server.c)

### 5.8 Cluster

Cluster mode is deeply integrated with command dispatch and DB indexing.

Key interactions:

- `processCommand()` asks `getNodeByQuery()` whether the local node should serve the request
- redirects are returned through `clusterRedirectClient()`
- key placement uses slot-aware DB indexing in `getKVStoreIndexForKey()`

Important entry points:

- `clusterInit()`
- `clusterInitLast()`
- `clusterCron()`
- `getNodeByQuery()`
- `clusterRedirectClient()`

#### Slot-Aware Command Routing

The central decision point for cluster request routing is `getNodeByQuery()` in [`src/cluster.c`](src/cluster.c).

This function assumes the client's target slot has already been determined and then answers:

- can this node serve the command?
- should the request be redirected?
- is the command invalid because it spans slots or hits an unstable slot state?

The function handles several important cases:

- single-slot commands
- multi-key same-slot commands
- `EXEC` inheriting the slot of the queued transaction
- cross-slot rejections
- migrating/importing slot checks
- read-only replica reads
- cluster-down handling

This is the runtime heart of cluster command routing.

The decision model looks roughly like:

```text
command parsed
  -> determine slot / detect cross-slot or no-key case
  -> getNodeByQuery()
      -> return myself
      -> return remote node + MOVED/ASK
      -> return NULL + CROSSSLOT / TRYAGAIN / CLUSTERDOWN
```

`clusterRedirectClient()` then converts those routing outcomes into actual client-visible protocol errors such as `-MOVED`, `-ASK`, `-CROSSSLOT`, `-TRYAGAIN`, and `-CLUSTERDOWN`.

#### Slot Stability and Migration Semantics

One of the most subtle parts of cluster behavior is that same-slot is not always sufficient for safe execution.

`getNodeByQuery()` also checks:

- whether the slot is migrating away from this node
- whether the slot is being imported
- whether all needed keys are present locally
- whether the client supplied `ASKING` correctly

This is why a request can fail with `TRYAGAIN` even when all keys hash to the same slot.

The important conceptual distinction is:

- `MOVED`: stable ownership is elsewhere
- `ASK`: ownership is moving and the destination can serve this request conditionally
- `TRYAGAIN`: slot instability or partial key presence makes execution unsafe right now

#### Cluster and the DB Layer

Cluster is not implemented as a thin routing shim in front of a slot-agnostic keyspace.

It also affects storage layout:

- `getKVStoreIndexForKey()` in [`src/db.c`](src/db.c) uses `getKeySlot()` in cluster mode
- `serverDb.keys` and related stores are slot-aware through `kvstore`
- some migration/import logic depends on slot-specific iteration and bookkeeping

This matters because cluster concerns appear in both:

- command admission/routing
- the physical organization of key containers

#### Cluster Lifecycle and Background Maintenance

Cluster startup is split between:

- `clusterInit()` before late initialization
- `clusterInitLast()` after listeners are created

At runtime, periodic cluster work runs from `clusterCron()` scheduled out of `serverCron()`.

This cron path is responsible for ongoing membership, liveness, and routing-state maintenance. Even if you are only changing command behavior, cluster correctness often depends on assumptions maintained by this background machinery.

#### Blocked Client Interaction

Cluster logic also reaches into blocked-client handling.

For example, `clusterRedirectBlockedClientIfNeeded()` in [`src/cluster.c`](src/cluster.c) exists to avoid clients waiting forever on a slot this node no longer serves.

That is an important architectural signal: cluster state is not only checked at command entry, it also influences long-lived waiting client state.

What to read first:

- `getNodeByQuery()`
- `clusterRedirectClient()`
- the cluster checks inside `processCommand()`

Common contributor pitfalls:

- assuming “same slot” is enough without considering migration/import stability
- forgetting that `EXEC` routing is transaction-wide, not just `EXEC` itself
- changing DB-level key placement assumptions without checking cluster `kvstore` indexing
- handling blocked clients without considering slot ownership changes

### 5.9 Modules

The module system in [`src/module.c`](src/module.c) is large because it exposes substantial integration points.

Capabilities include:

- custom commands
- subcommands
- blocked clients
- server event subscriptions
- keyspace notifications
- custom config
- custom data types
- fork and background work
- scripting engine registration

Lifecycle:

1. `moduleInitModulesSystem()`
2. config parser enqueues `loadmodule`
3. `moduleInitModulesSystemLast()`
4. `moduleLoadFromQueue()`

Module commands are dispatched via `ValkeyModuleCommandDispatcher()`.

The module subsystem is one of the hardest areas to fully understand because it spans command execution, auth, blocking, persistence, events, and config.

### 5.10 Scripting and Functions

Scripting is split between:

- EVAL-style scripts in [`src/eval.c`](src/eval.c)
- server-side functions in [`src/functions.c`](src/functions.c)
- common execution/runtime state in [`src/script.c`](src/script.c)
- engine management in [`src/scripting_engine.c`](src/scripting_engine.c)

Startup hooks:

- `scriptingEngineManagerInit()`
- `functionsInit()`
- `evalInit()`

Important detail:

- command execution paths explicitly check whether the server is inside a yielding long-running script/module and restrict what other clients may do in that state

## 6. Key End-to-End Traces

### 6.1 Server Startup

```text
main
  -> initServerConfig
  -> ACLInit
  -> moduleInitModulesSystem
  -> connTypeInitialize
  -> loadServerConfig
  -> initServer
  -> clusterInit
  -> moduleInitModulesSystemLast
  -> moduleLoadFromQueue
  -> ACLLoadUsersAtStartup
  -> initListeners
  -> clusterInitLast
  -> InitServerLast
  -> aofLoadManifestFromDisk
  -> loadDataFromDisk
  -> aeMain
```

### 6.2 Accepting a Client

```text
transport accept handler
  -> acceptCommonHandler
  -> createClient
  -> connAccept(..., clientAcceptHandler)
```

Key structs involved:

- `connection`
- `client`
- `connListener`

### 6.3 Reading and Executing a Command

```text
readQueryFromClient
  -> readToQueryBuf
  -> processInputBuffer
      -> parseInputBuffer / consumeCommandQueue
      -> processCommandAndResetClient
          -> processCommand
              -> call
```

State transitions:

- socket bytes become `querybuf`
- parser output becomes `argv`/`argc` and `parsed_cmd`
- command checks assign `cmd`, `realcmd`, `lastcmd`
- reply is queued in output buffers

### 6.4 Mutating the Dataset

Typical write flow:

```text
processCommand
  -> command implementation
      -> lookupKeyWrite / setKey / dbDelete
      -> signalModifiedKey
      -> notifyKeyspaceEvent
  -> call post-processing
      -> propagation decision
      -> monitor / stats / tracking / afterCommand
```

### 6.5 Loading Persisted State

```text
loadDataFromDisk
  -> loadAppendOnlyFiles
or
  -> rdbLoad
      -> rdbLoadRioWithLoadingCtx
      -> rdbLoadObject
      -> dbAddRDBLoad
```

### 6.6 Cluster Routing

```text
processCommand
  -> getNodeByQuery
  -> if remote owner:
       clusterRedirectClient
```

### 6.7 Replica Synchronization

```text
replica connection setup
  -> syncWithPrimary
      -> handshake states
      -> send PSYNC
      -> partial resync accepted
         or
         full resync required
  -> if full resync:
       receive / load RDB
  -> transition to command stream replication
  -> replicationSendAck
```

Key structs involved:

- `client` with `repl_data`
- global `server.repl_*` state
- `replBufBlock`

### 6.8 Cluster Redirection and Slot Migration

```text
processCommand
  -> slot already computed for client
  -> getNodeByQuery
      -> stable local slot: execute locally
      -> stable remote slot: MOVED
      -> importing/migrating slot: ASK or TRYAGAIN
      -> invalid multi-slot request: CROSSSLOT
      -> cluster unhealthy: CLUSTERDOWN
```

This is one of the best traces to study if you want to understand how routing, key lookup expectations, transaction semantics, and migration state interact in one place.

## 7. Cross-Cutting Concerns

### Memory Management and Object Lifetime

- `zmalloc` is used throughout
- `robj` has refcount semantics with special cases for shared/static objects
- embedding reduces allocation count and improves locality
- background save/load logic is careful about copy-on-write costs

### Concurrency Model

- Main command engine is centralized
- I/O threads assist around reads/writes/parsing
- modules may create more concurrency edges via thread-safe contexts and wakeup pipe integration

### Event-Driven Execution

- `ae.c` provides generic file/time event multiplexing
- server-specific policy lives in `beforeSleep()`, `afterSleep()`, and `serverCron()`

### Blocking vs Background Work

- blocked clients are represented explicitly in client state
- some disk and persistence operations are moved to background threads or child processes
- module APIs also support blocked client workflows

### Error Handling

- config errors fail hard during startup
- command rejection uses `rejectCommand*()` helpers
- runtime errors are also counted in `server.errors`

### Persistence and Replication Consistency

- command propagation is decided centrally in `call()`
- the same command flow can feed AOF and replica streams
- persistence and replication concerns are not isolated from the core execution path

### Extension Points

- loadable modules
- module-defined commands and data types
- module event hooks
- pluggable scripting engines

## 8. Testing Strategy

### `src/unit/`

This suite covers low-level logic such as:

- containers and encodings
- utility functions
- object behavior
- networking helpers
- kvstore behavior

Examples:

- `test_object.cpp`
- `test_networking.cpp`
- `test_kvstore.cpp`
- `test_quicklist.cpp`

### `tests/`

This suite covers end-to-end behavior:

- commands and protocol
- persistence
- replication
- cluster and failover
- ACL
- pubsub
- module API

Examples:

- `tests/unit/protocol.tcl`
- `tests/integration/replication.tcl`
- `tests/integration/rdb.tcl`
- `tests/unit/moduleapi/*.tcl`

### Practical Reading Advice

- when changing storage/object behavior, read `src/unit/` first
- when changing command semantics or distributed behavior, read `tests/unit/` and `tests/integration/`

## 9. Suggested Reading Order

### First Pass

1. [`src/server.h`](src/server.h)
2. [`src/server.c`](src/server.c)
3. [`src/networking.c`](src/networking.c)
4. [`src/db.c`](src/db.c)
5. [`src/object.c`](src/object.c)

### Second Pass

1. [`src/config.c`](src/config.c)
2. [`src/ae.c`](src/ae.c)
3. [`src/rdb.c`](src/rdb.c)
4. [`src/aof.c`](src/aof.c)
5. [`src/replication.c`](src/replication.c)

### Third Pass

1. [`src/cluster.c`](src/cluster.c)
2. [`src/module.c`](src/module.c)
3. [`src/eval.c`](src/eval.c)
4. [`src/functions.c`](src/functions.c)
5. type-specific files like [`src/t_string.c`](src/t_string.c) and [`src/t_hash.c`](src/t_hash.c)

### Easiest Entry Points

- command lookup and dispatch
- DB key lookup/mutation
- event loop hooks

### Hardest but Important Areas

- module subsystem
- replication state machine
- cluster routing and migration
- persistence load/save internals

## 10. Glossary

- `robj`: internal heap object used to represent values
- `serverDb`: one logical DB’s keyspace and related metadata
- `client`: connection plus parser/execution/output state
- `server`: global singleton server state
- `AOF`: append-only persistence
- `RDB`: snapshot persistence format
- `PSYNC`: replication synchronization protocol
- `kvstore`: internal key container used by DBs and some pubsub paths
- `RESP2` / `RESP3`: supported client protocol versions
- `beforeSleep` / `afterSleep`: event loop hooks where generic polling becomes server runtime behavior

## Notes

- This document is intentionally centered on the standalone server runtime rather than Sentinel-specific behavior.
- Inference: command metadata generation details are driven by the generated command table and command-definition inputs, but this document focuses on the runtime use of that metadata rather than the generation pipeline itself.
