package benchy

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"
)

type QueryParams struct {
	Host  string
	Start time.Time
	End   time.Time
}

type QueryResult struct {
	Duration float64
	Host     string
	Error    error
}

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

type Stats struct {
	ExecCount      int
	FailedCount    int
	Sum            float64
	Min            float64
	Max            float64
	Mean           float64
	Median         float64
	Percentile95th float64
	Percentile99th float64
}

// listen to results and accumulate then print.
func CollectResult(errCh chan error, result chan QueryResult, statCh chan Stats) {
	durations := []float64{}

	execCount := 0
	sqlFailure := 0

	min := math.Inf(1)
	max := math.Inf(-1)

	var sum float64 = 0

	for r := range result {
		if r.Error != nil {
			errCh <- fmt.Errorf("sql err: %w", r.Error)
			sqlFailure++

			continue
		}

		execCount++

		durations = append(durations, r.Duration)

		min = math.Min(min, r.Duration)
		max = math.Max(max, r.Duration)
		sum += r.Duration
	}

	sort.Float64s(durations)

	statCh <- Stats{
		ExecCount:      execCount,
		FailedCount:    sqlFailure,
		Sum:            sum,
		Min:            min,
		Max:            max,
		Mean:           sum / float64(execCount),
		Median:         percentile(durations, 50),
		Percentile95th: percentile(durations, 95),
		Percentile99th: percentile(durations, 99),
	}
}

const (
	maxPercentile        = 100
	minPercentile        = 0
	minPercentileDataLen = 2
)

func percentile(data []float64, p float64) float64 {
	if p < minPercentile {
		return math.NaN()
	}

	if p > maxPercentile {
		return math.NaN()
	}

	n := float64(len(data))

	if n < minPercentileDataLen {
		return math.NaN()
	}

	rank := (p/100)*(n-1) + 1
	ri := float64(int64(rank))
	rf := rank - ri
	i := int(ri) - 1

	return data[i] + rf*(data[i+1]-data[i])
}

const resultMsg = `#summary:
total_count: 	%d
exec_count:	%d
sql_failure:	%d
parse_failure:	%d
#durations:
total:	%.2fms
min:	%.2fms
max:	%.2fms
mean:	%.2fms
median:	%.2fms
95th:	%.2fms
99th:	%.2fms
`

func FormatStat(parseFailure int, stat Stats) string {
	return fmt.Sprintf(
		resultMsg,
		stat.ExecCount+stat.FailedCount+parseFailure,
		stat.ExecCount,
		stat.FailedCount,
		parseFailure,
		stat.Sum,
		stat.Min,
		stat.Max,
		stat.Mean,
		stat.Median,
		stat.Percentile95th,
		stat.Percentile99th,
	)
}
