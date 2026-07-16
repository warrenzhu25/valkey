# Proposal — An io_uring I/O backend (D5, the I/O-bound branch)

**Status: pre-issue draft.** This is the **D5 / Stage 5** counterpart to
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) (Stage 4). Stage 0's Q1 is a
fork in the road: **execution-bound → slot-per-thread; I/O-bound → io_uring.** This note is
the "what if I/O-bound wins" side — what io_uring would and would not buy Valkey, and at
what cost. It is explicitly *not* a recommendation to build; it's the analysis Stage 0's
"I/O-bound" result would trigger.

Facts about Valkey's current I/O layer are verified (`file:line`). Facts about io_uring
behavior and other systems' results are external knowledge, marked where they matter.

Gated by [proposal-stage0-measurement.md](proposal-stage0-measurement.md): **do not build
this before Q1 says syscalls dominate a saturated core.** On an execution-bound workload,
io_uring is irrelevant.

---

## 1. The one fact that shapes everything

Valkey's entire I/O layer is **readiness-based**. The `ae` event loop has four backends —
`ae_epoll.c`, `ae_kqueue.c`, `ae_evport.c`, `ae_select.c` — and every one answers the
question *"which fds are ready?"* and then the code calls `read`/`write` itself
(`AE_READABLE`/`AE_WRITABLE` dispatch, `src/ae.c:197`). There is **no io_uring backend**
today.

io_uring is the opposite model: **completion-based**. You *submit* I/O operations into a
ring and later *reap completions* — the kernel does the read/write and tells you it's done.
This is not a fifth `ae` backend you can drop in next to epoll; it inverts who performs the
I/O. That inversion is the source of both io_uring's benefit and its retrofit cost.

## 2. What io_uring actually reduces — and what it doesn't

Per Stage 0's framing, a non-pipelined small-command workload spends its main-thread cycles
in network **syscalls + kernel TCP stack + RESP parsing**. io_uring attacks a specific
slice of that, not all of it.

**Reduces:**
- **Syscall count / mode-switch overhead.** Batch every ready connection's `read`/`write`
  into a single `io_uring_enter` instead of one syscall per op. With **SQPOLL** (a kernel
  thread polling the submission ring) the hot path can approach **zero** syscalls.
- **Per-op kernel setup:** registered files/buffers skip repeated fd refcounting; multishot
  `accept`/`recv` turn one submission into many completions; provided-buffer pools let the
  kernel pick a buffer at completion time (good for many idle-ish connections).

**Does not reduce (the honest half):**
- **Kernel TCP/IP stack cost** — the actual per-packet processing is identical whether you
  call `read()` or submit to io_uring. This is often the *larger* part of "network cost,"
  and io_uring does nothing for it.
- **RESP parsing** — pure userspace CPU, untouched.
- **Copies** — unless you adopt zero-copy send (`send_zc`), which for the small buffers
  Valkey usually writes is not a clear win and carries its own completion-tracking overhead.

**So the win is "make the syscall layer cheap," not "make networking free."** It is largest
with **many connections doing small I/O** (high syscall count) and shrinks as pipelining or
large values already amortize syscalls away.

## 3. The catches specific to Valkey

1. **Readiness → completion is a rewrite, not a plugin.** You *can* run io_uring in
   poll-emulation mode (`IORING_OP_POLL_ADD` to mimic epoll readiness) — but that keeps the
   `read`/`write` syscalls and captures almost none of the benefit. A real win requires
   reworking the I/O layer to be completion-driven: client read/write paths, the buffer
   lifecycle (partial reads, reply lists), and the `ae` abstraction itself. That is a
   substantial change to load-bearing code, and it must ship **alongside** the epoll backend
   (§3.3), not replace it.

2. **It overlaps with I/O-threads, which Valkey already has.** `io_threads.c` +
   `design-docs/io-threads.md` already attack the same bottleneck a *different* way —
   **spread** syscall cost across cores rather than **reduce** it per-op. If io-threads
   already relieve the I/O bottleneck on the measured workload, io_uring's marginal gain is
   correspondingly smaller. The genuinely powerful design is io_uring *inside* the io-threads
   (both levers at once), but that compounds the complexity of both. **Any io_uring
   evaluation must be run with io-threads already tuned**, or it will over-credit io_uring
   for work io-threads would have done anyway.

3. **Kernel version + security gate.** The good features (multishot, provided buffers,
   zero-copy) need recent kernels (≈5.19 / 6.x). io_uring has also been a prolific source of
   kernel CVEs — enough that Google, Android, and some hardened/container environments
   **disable it outright**. Valkey must keep running on old and io_uring-less kernels, so
   this is necessarily an **optional, runtime-gated backend** with a clean epoll fallback —
   i.e. permanent maintenance of two I/O paths.

4. **SQPOLL is not free.** The zero-syscall mode burns a **dedicated kernel poller thread
   (a core)** per ring. That is a throughput-vs-CPU-efficiency trade, not a pure win — you
   spend a core to save syscalls. It shines at high connection counts and hurts at low load.

5. **Nothing for execution-bound workloads.** If Q1 says the bottleneck is `call()`
   (slot-per-thread §3.1's favorable cell), io_uring changes nothing. This whole note is
   dead on that branch.

### 3.3 It has to be a fifth backend, gated

The only viable shape: add `ae_uring.c` as an additional `ae` backend selected at
runtime/build time, with epoll as the fallback whenever io_uring is absent, disabled by
policy, or on too old a kernel. This preserves Valkey's portability contract and lets the
feature ship dormant — but it also means the completion-model reshaping of the client I/O
paths must be expressed *behind* the `ae` interface without regressing the readiness path.
Reconciling a completion API under a readiness-shaped abstraction is the core engineering
risk.

## 4. Calibrated expectation

- **Best case** (many connections, small non-pipelined commands, io-threads off): a
  **meaningful but bounded** gain — order single-digit-% to ~2× throughput on the
  syscall-bound portion **(external, approximate; consistent with Redis's own io_uring
  experiments)**.
- **With io-threads already on:** less, because the bottleneck is partly already spread.
- **Pipelined or large-value workloads:** little — syscalls are already amortized.
- **Execution-bound workloads:** nothing.

**Dragonfly is not a counter-example.** Its strong io_uring numbers come from a *from-scratch*
thread-per-core + io_uring + fibers architecture (helio), where io_uring is load-bearing
because the whole design is completion-oriented. You cannot retrofit that result onto Valkey
by swapping the I/O backend alone — the gain there is the architecture, not the syscall
interface in isolation. **(vendor/architecture read, not measured here.)**

## 5. Phasing (only entered if Stage 0 Q1 = I/O-bound)

1. **Measurement gate.** Stage 0 §5.B/§5.C shows syscalls dominate a saturated core *with
   io-threads already tuned*. If not, stop — this is the wrong lever.
2. **Prototype `ae_uring.c` in true completion mode** (not poll-emulation) for the read and
   write paths only, epoll fallback intact. Benchmark the §2 sweet-spot workload (many
   connections, small non-pipelined ops) head-to-head against tuned io-threads.
3. **Decide composition with io-threads:** io_uring-per-io-thread vs io_uring on the main
   thread only. This is the real architectural fork and should be driven by step-2 numbers.
4. *(Only if the numbers hold)* registered buffers, multishot recv/accept, and an evaluated —
   not assumed — SQPOLL and zero-copy-send.

Each step is independently killable, and step 1 can kill the whole thing before any I/O-layer
code is written.

## 6. What would make me abandon this

- **Stage 0 says execution-bound** (§5, the §3.1 favorable cell) — then Stage 4
  (slot-per-thread) is the bet and this note is moot.
- **Tuned io-threads already capture most of the I/O headroom**, leaving io_uring a thin
  marginal gain not worth a permanent second I/O path + kernel/security gate.
- **The syscall slice is small** relative to kernel-stack + parsing (io_uring can't touch
  those), so even eliminating syscalls entirely moves the needle little.
- **Security/kernel constraints** in the target deployments (containers, hardened hosts,
  older kernels) mean the fallback path is what most users actually run — making io_uring a
  benchmark-only feature.

Any one of these means Valkey's existing io-threads bet was the right call and io_uring is
not worth its complexity.

## 7. Code anchors

| Thing | Where |
|---|---|
| Readiness-model event loop (the thing io_uring inverts) | `src/ae.c:197` (`AE_READABLE`/`AE_WRITABLE` dispatch) |
| Existing backends (io_uring would be a 5th) | `src/ae_epoll.c`, `src/ae_kqueue.c`, `src/ae_evport.c`, `src/ae_select.c` |
| The overlapping lever already shipped | `src/io_threads.c`, `design-docs/io-threads.md` |
| The gate | [proposal-stage0-measurement.md](proposal-stage0-measurement.md) §5.B (syscall profile), §5.C (io-threads sweep) |
| The other branch | [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §3.1 favorable cell (execution-bound) |

## 8. Sources

- Linux `io_uring` interface and features (SQPOLL, registered buffers, multishot, `send_zc`)
  — kernel docs / `liburing`; feature/kernel-version specifics are **(approx)**, verify
  against the target kernel.
- Redis io_uring experiments (bounded-gain, complexity findings) — **(external, approximate)**.
- Dragonfly / helio io_uring-native architecture — vendor/architecture read, not measured
  here; see [09-dragonfly-snapshot-model.md](09-dragonfly-snapshot-model.md) for the
  confidence caveat pattern.
</content>
