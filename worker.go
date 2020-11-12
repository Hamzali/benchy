package benchy

import (
	"sync"
	"time"
)

// initialize workers and setup channels.
func StartWorkers(workerCount int, work func(q QueryParams) error) ([]chan QueryParams, chan QueryResult) {
	var workerChs []chan QueryParams

	var workerWg sync.WaitGroup

	result := make(chan QueryResult)

	workerWg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		workerChs = append(workerChs, make(chan QueryParams))
		go func(ch chan QueryParams) {
			defer workerWg.Done()

			for q := range ch {
				now := time.Now()
				err := work(q)
				elapsed := time.Since(now)
				result <- QueryResult{
					Host:     q.Host,
					Duration: float64(elapsed.Milliseconds()),
					Error:    err,
				}
			}
		}(workerChs[i])
	}

	// close result channel after every worker finishes
	go func() {
		workerWg.Wait()
		close(result)
	}()

	return workerChs, result
}
