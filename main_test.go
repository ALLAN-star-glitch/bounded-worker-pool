package main

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestRunPool(t *testing.T) {
	// Define table-driven test cases covering normal completion and early timeout
	tests := []struct {
		name        string
		numWorkers  int
		numJobs     int
		timeout     time.Duration
		wantErrJobs bool
	}{
		{
			name:        "all jobs complete before timeout",
			numWorkers:  3,
			numJobs:     6,
			timeout:     500 * time.Millisecond, // Plenty of time for 6 jobs (50ms each)
			wantErrJobs: false,
		},
		{
			name:        "early cancellation aborts remaining jobs",
			numWorkers:  2,
			numJobs:     6,
			timeout:     75 * time.Millisecond, // Cuts off after the first batch of 2 jobs
			wantErrJobs: true,
		},
	}

	// Loop over each test case as an isolated subtest
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Record active goroutine count before running the pool
			baselineGoroutines := runtime.NumGoroutine()

			// Generate the requested number of dummy jobs
			var jobs []Job
			for i := 1; i <= tc.numJobs; i++ {
				jobs = append(jobs, Job{ID: i})
			}

			// Create a context with the test case's timeout
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			// Execute the worker pool
			results := RunPool(ctx, tc.numWorkers, jobs)

			// Inspect results to ensure any error is specifically DeadlineExceeded
			var hasErr bool
			for _, r := range results {
				if r.Err != nil {
					if !errors.Is(r.Err, context.DeadlineExceeded) {
						t.Fatalf("unexpected error: %v", r.Err)
					}
					hasErr = true
				}
			}

			// Verify whether timeout errors occurred as expected
			if hasErr != tc.wantErrJobs {
				t.Errorf("got error jobs = %v, want %v", hasErr, tc.wantErrJobs)
			}

			// Allow Go scheduler a brief moment to clean up exited goroutines, then check for leaks
			time.Sleep(10 * time.Millisecond)
			if got := runtime.NumGoroutine(); got > baselineGoroutines {
				t.Errorf("goroutine leak detected: baseline=%d, after=%d", baselineGoroutines, got)
			}
		})
	}
}
