# Proposal — Main-thread CPU-time distribution without a profiler

**Status: pre-issue draft.** A Valkey-native proposal (not a Dragonfly study), so unlike
most notes here it carries verified `file:line` anchors throughout, not "vendor says so."

Companion to [proposal-stage0-measurement.md](proposal-stage0-measurement.md): that plan
gates the whole slot-per-thread roadmap on a lab measurement of "is the main thread
execution-bound or I/O-bound?" This note argues that Valkey can answer that question
**in-process, on production traffic, from `INFO`** — because it already ships ~70% of the
machinery — and specifies the small gap to close.

---

## 1. The idea in one paragraph

Valkey already brackets each event-loop iteration with `getMonotonicUs()` and banks the
time into typed buckets (`duration_stats[]`, `src/server.h:1958`). It reports command,
cron, and AOF time separately via `INFO`. What it does **not** break out is **networking
I/O** and **idle/poll wait** — which are exactly the two numbers that decide the Stage 0
question. Add those two buckets (and, optionally, a true-CPU-clock refinement) and Valkey
self-reports a coarse main-thread CPU distribution with **no external profiler**, live, in
production.

## 2. What Valkey already has

Event-loop phase accounting exists and is shipped. `server.el_start` is set in
**`afterSleep()`** (`src/server.c:2075`) — *after* `epoll_wait` returns — and the
iteration's duration is banked at the end of **`beforeSleep()`** (`src/server.c:2013`). So
the "whole loop" bucket already measures only the **awake** portion and structurally
excludes the poll wait.

| Bucket (`src/latency.h:106`) | Captures | INFO field (`src/server.c:6563`+) |
|---|---|---|
| `EL_DURATION_TYPE_EL` | whole awake iteration (wake → next sleep) | `eventloop_duration_sum` |
| `EL_DURATION_TYPE_CMD` | `call()` execution (`src/server.c:4023`) | `eventloop_duration_cmd_sum` |
| `EL_DURATION_TYPE_CRON` | `serverCron` + `beforeSleep`, **"excluding IO and AOF"** | `eventloop_duration_cron_sum` |
| `EL_DURATION_TYPE_AOF` | AOF flush in the loop (`src/server.c:1966`) | `eventloop_duration_aof_sum` |

Each bucket is a `durationStats {cnt, sum, max}` (`src/latency.h:99`), fed by
`durationAddSample()` (`src/latency.h:114`), with instantaneous per-second variants
(`instantaneous_eventloop_duration_usec`). Separately, `INFO` already reports true
main-thread CPU totals via `getrusage(RUSAGE_THREAD)` → `used_cpu_{sys,user}_main_thread`
(`src/server.c:6711`).

So today, from `INFO` alone: *of the main thread's awake time, X% command, Y% cron, Z% AOF.*

## 3. The three gaps

1. **Networking is not a bucket — it's a residual.** The `CRON` type comment says it
   explicitly excludes I/O (`src/latency.h:109`), and there is no I/O type. Socket
   read/parse and reply-write time is only recoverable as `EL − CMD − CRON − AOF`. That
   residual is precisely the "I/O" side of the execution-vs-I/O question, so leaving it
   implicit defeats the measurement. **It also hides the effect of `io-threads`**, which
   move read/write *off* the main thread — you cannot see that shift unless main-thread I/O
   is named.

2. **Idle/poll wait is not reported.** The cleanest "saturated vs waiting" signal is time in
   `epoll_wait`. It is *derivable* (wall-clock − `eventloop_duration_sum`) but not a
   first-class metric. Report it, and `busy / (busy + idle)` becomes a live saturation gauge.

3. **It is wall time, not CPU time.** Every bucket uses `getMonotonicUs()` (wall). On a busy
   single thread wall ≈ CPU, but if the OS deschedules the thread mid-phase (noisy neighbor,
   cgroup CPU throttling) wall **over-counts** CPU, and I/O-wait can masquerade as compute.

## 4. Design

**4.1 Add explicit buckets.** Introduce `EL_DURATION_TYPE_IO_READ` and
`EL_DURATION_TYPE_IO_WRITE` (or a single `_IO`), sampled around the read/parse and
reply-flush phases in `beforeSleep`/the file-event handlers, using the same
`durationAddSample()` path. Add a `poll_wait` accumulator recorded across the sleep window
(between `beforeSleep` end and `afterSleep` start — the two timestamps already exist). Once
those are present the buckets **sum to wall-clock**, so the distribution is complete and
self-checking (residual ≈ 0 is a correctness assertion).

**4.2 Report a normalized split.** Add an `INFO` block (extend the `eventloop` section, or a
new `cpustats`) giving each bucket as a **percentage of a rolling window**, plus the
instantaneous per-second form so it can be watched live. The percentages, not the raw sums,
are what an operator reads to answer Stage 0.

**4.3 True-CPU refinement (optional, second phase).** Bracket the coarse phase boundaries
with `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` in addition to the monotonic wall clock, and
report both a wall and a thread-CPU figure for the loop. The **gap between them is
descheduling** — a direct, in-process signal that the thread was starved rather than busy,
which no amount of wall-clock bucketing can distinguish. Keep the CPU clock at coarse
boundaries only (poll / read / exec / write / cron), never per-command — see §5.

## 5. Overhead

The bracketing tax is already paid: two monotonic reads per iteration plus one per command.
Adding I/O and idle buckets is a few more `getMonotonicUs()` per loop — negligible, and of
the same class Valkey already accepted. `CLOCK_THREAD_CPUTIME_ID` is usually vDSO-fast on
Linux but **not guaranteed** across platforms, so restrict it to coarse phase boundaries;
per-command CPU-clock reads could add up at high throughput. If even the coarse cost is a
concern, gate 4.3 behind a config (default off), the way `cluster-slot-stats-enabled` gates
the per-slot machinery.

## 6. Honest limits

- **This is phase accounting, not a function-level profile.** It says "command execution is
  60% of CPU," not *which command* or *which function inside `call()`*. For that you still
  need `perf`. But `INFO commandstats` already decomposes the CMD bucket per-command, and
  the coarse execution/I/O/idle split is exactly Stage 0's granularity — no finer needed to
  answer the gating question.
- **Wall vs CPU (see §4.3).** Without the CPU-clock refinement, the distribution can misread
  a throttled thread as compute-heavy. Ship 4.1/4.2 first (they answer the question on an
  unthrottled box), add 4.3 for hostile/shared environments.
- **I/O-threads make "main-thread I/O" a moving target.** With `io-threads > 1`, read/write
  is partly on worker threads; the main-thread I/O bucket shrinks and the *worker* threads'
  time is invisible to this scheme. A complete picture would need the same buckets per I/O
  thread — out of scope for a first cut, but worth naming so the numbers aren't misread.
- **Attribution boundaries are approximate.** `beforeSleep` interleaves I/O and bookkeeping;
  drawing a clean I/O/cron line there requires care and some judgment calls about which side
  a given step belongs to.

## 7. Why this strengthens Stage 0

[proposal-stage0-measurement.md](proposal-stage0-measurement.md) frames the gating
measurement as a lab exercise: run a benchmark under `perf`, classify execution-bound vs
I/O-bound, decide whether slot-per-thread is worth pursuing. Closing these three gaps turns
that one-shot lab number into an **always-on, production-observable metric**: any operator
reads the execution/I/O/idle split from `INFO` on their *real* workload. The whole roadmap
would then be gated on production data, not synthetic benchmarks — a materially stronger
basis for a multi-year bet. And it is independently useful the moment it lands (capacity
planning, "why is my main thread hot") regardless of whether slot-per-thread ever ships.

## 8. Phasing

1. **Add the I/O and idle buckets (4.1) + normalized `INFO` split (4.2).** Self-contained,
   independently useful, answers Stage 0 on an unthrottled host. Assert buckets sum to wall.
2. **Add the `CLOCK_THREAD_CPUTIME_ID` wall-vs-CPU refinement (4.3)**, config-gated, for
   throttled/containerized environments.
3. *(Optional)* Extend the same buckets per I/O thread for a whole-process picture.

Stopping after step 1 already delivers the metric and the Stage 0 answer.

## 9. What would make me abandon this

- **The residual I/O bucket turns out to be un-cleanly separable** from cron/bookkeeping in
  `beforeSleep` without invasive restructuring, making the "networking" number too fuzzy to
  trust — at which point `perf` in a lab is the honest tool and this is false precision.
- **The measured distribution is dominated by idle** on realistic workloads, confirming
  Valkey is latency/I/O-bound — which would be a *successful* use of the metric, but also
  the finding that ends the slot-per-thread roadmap (per Stage 0), making further
  instrumentation investment moot.

## 10. Code anchors

| Thing | Where |
|---|---|
| Duration buckets array | `duration_stats[]` `src/server.h:1958` |
| Bucket types (add I/O + idle here) | `DurationType` enum `src/latency.h:106` |
| `durationStats {cnt,sum,max}` / sampler | `src/latency.h:99`, `durationAddSample` `src/latency.h:114` |
| Loop bracket start (after poll) | `server.el_start = getMonotonicUs()` in `afterSleep` `src/server.c:2075` |
| Loop bracket bank (before poll) | `EL_DURATION_TYPE_EL` sample `src/server.c:2013` |
| Command time sample | `EL_DURATION_TYPE_CMD` `src/server.c:4023` |
| AOF time sample | `src/server.c:1966` |
| INFO eventloop fields | `src/server.c:6563`, `src/server.c:6834` |
| Existing main-thread CPU totals | `getrusage(RUSAGE_THREAD)` → `used_cpu_*_main_thread` `src/server.c:6711` |
| Where I/O work happens (to bracket) | `beforeSleep` `src/server.c:1854`, io-threads `src/io_threads.c` |
</content>
