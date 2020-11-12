package benchy

import "fmt"

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
