package benchy

import (
	"fmt"
	"math"
	"sort"
)

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
