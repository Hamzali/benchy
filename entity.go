package benchy

import (
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
