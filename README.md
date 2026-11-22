# Bounded Worker Pool & Concurrency Lab (Go)

A self-contained Go implementation demonstrating bounded concurrency, graceful `context` cancellation, race-free channel coordination, and goroutine leak detection.

## Repository Structure

```text
bounded-worker-pool/
├── go.mod          # Go module definition
├── main.go         # Bounded worker pool implementation and runnable demo
└── main_test.go    # Table-driven unit tests and goroutine leak checks
```

## Architecture & Execution Flow

```text
[jobs slice] ──▶ (jobCh) ──▶ [Worker 1] ──┐
                          ──▶ [Worker 2] ──┼──▶ (resCh) ──▶ [results slice]
                                           │
                    [ctx.Done() alarm] ────┘ (Aborts in-flight work immediately)
```

1. **Buffered Input & Output Channels:** `jobCh` and `resCh` are sized to `len(jobs)` to prevent sender blocking during queue population and result collection.
2. **Bounded Goroutine Pool:** Spawns a fixed number of worker goroutines (`numWorkers`), preventing unbounded resource consumption under heavy I/O workloads.
3. **Graceful Cancellation:** Each worker listens to `<-ctx.Done()` both while idle and during simulated I/O (`time.After`), aborting immediately when a timeout or cancellation occurs.
4. **Safe Channel Lifecycle:**
   * Only the producer closes `jobCh` after loading all tasks.
   * `wg.Wait()` blocks until all workers exit via `defer wg.Done()`.
   * `close(resCh)` executes strictly after `wg.Wait()` to guarantee no worker attempts to send on a closed channel.

## Lab Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/ALLAN-star-glitch/bounded-worker-pool.git
cd bounded-worker-pool
```

### 2. Run the Demo with Race Detection
Execute the worker pool (`2` workers processing `6` jobs at `50ms` each against a `120ms` timeout) with Go's race detector enabled:
```bash
go run -race main.go
```

### 3. Run Table-Driven Tests & Goroutine Leak Checks
Execute the test suite with race detection and statement coverage:
```bash
go test -race -v -cover ./...
```

### 4. What Each Test Verifies
* **Normal Completion (`all jobs complete before timeout`):** Verifies all jobs succeed cleanly when the context deadline exceeds total processing time.
* **Early Cancellation (`early cancellation aborts remaining jobs`):** Verifies in-flight jobs abort immediately with `context.DeadlineExceeded` when the timeout fires early.
* **Zero Goroutine Leaks:** Asserts `runtime.NumGoroutine()` returns to its pre-run baseline after pool termination.
