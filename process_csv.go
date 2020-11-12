package benchy

import (
	"io"
	"time"
)

// parse input rows and assign to workers.
func ProcessCsv(reader io.Reader, errCh chan error, workerChs []chan QueryParams) (int, error) {
	hostWorkerMap := map[string]int{}
	workerIndex := 0
	parseFailure := 0

	err := ParseCsv(reader, func(err error, host string, start, end time.Time) {
		if err != nil {
			errCh <- err
			parseFailure++

			return
		}

		w, ok := hostWorkerMap[host]
		if !ok {
			w = workerIndex
			hostWorkerMap[host] = w
			workerIndex = (workerIndex + 1) % len(workerChs)
		}

		ch := workerChs[w]

		ch <- QueryParams{
			Host:  host,
			Start: start,
			End:   end,
		}
	})

	return parseFailure, err
}
