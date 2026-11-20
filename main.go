package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Job represents a single unit of work to be processed.
type Job struct {
	ID int
}

// Result holds the outcome of a processed job.
type Result struct {
	JobID  int
	Worker int
	Err    error
}

// RunPool processes jobs concurrently using a fixed number of workers and respects context cancellation.
func RunPool(ctx context.Context, numWorkers int, jobs []Job) []Result {
	// Buffered channels hold all incoming jobs and outgoing results without blocking
	jobCh := make(chan Job, len(jobs))
	resCh := make(chan Result, len(jobs))

	// WaitGroup tracks active workers so we know when all goroutines have exited
	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		// Launch each worker in its own background goroutine, passing a copy of w
		go func(workerID int) {
			defer wg.Done() // Decrement counter when this worker exits
			for {
				select {
				case <-ctx.Done():
					// Stop immediately if the timeout/cancellation signal fired while waiting
					return
				case job, ok := <-jobCh:
					// If jobCh is closed and empty, no work remains—exit cleanly
					if !ok {
						return
					}
					// Simulate 50ms of I/O work while still listening for early cancellation
					select {
					case <-time.After(50 * time.Millisecond):
						resCh <- Result{JobID: job.ID, Worker: workerID}
					case <-ctx.Done():
						resCh <- Result{JobID: job.ID, Worker: workerID, Err: ctx.Err()}
						return
					}
				}
			}
		}(w)
	}

	// Push all jobs into the queue, then close jobCh to signal no more jobs are coming
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)

	// Wait for every worker to finish before safely closing the output channel
	wg.Wait()
	close(resCh)

	// Drain all collected results from the closed channel into a slice
	var results []Result
	for r := range resCh {
		results = append(results, r)
	}
	return results
}

func main() {
	// Create a context that automatically cancels after 120ms
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	// Run 6 jobs (50ms each) across 2 concurrent workers
	jobs := []Job{{1}, {2}, {3}, {4}, {5}, {6}}
	results := RunPool(ctx, 2, jobs)

	for _, r := range results {
		fmt.Printf("Job %d | Worker %d | Err: %v\n", r.JobID, r.Worker, r.Err)
	}
}
