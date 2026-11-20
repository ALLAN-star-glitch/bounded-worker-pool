# Bounded Worker Pool & Concurrency Lab (Go)

A self-contained Go implementation demonstrating bounded concurrency, graceful `context` cancellation, race-free channel coordination, and goroutine leak detection.

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

## Running & Testing

### Run the Demo with Race Detection
```bash
go run -race main.go
```

### Run Table-Driven Tests & Goroutine Leak Checks
```bash
go test -race -v -cover ./...
```

The test suite verifies:
* **Normal Completion:** All jobs complete cleanly before the context deadline.
* **Early Cancellation:** In-flight jobs abort with `context.DeadlineExceeded` when the timeout fires early.
* **Zero Goroutine Leaks:** `runtime.NumGoroutine()` returns to baseline after pool termination.
