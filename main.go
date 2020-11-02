package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type PostgresConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"db"`
	SSL      bool   `json:"ssl"`
}

type Config struct {
	File        string         `json:"-"`
	WorkerCount int            `json:"worker_count"`
	Postgres    PostgresConfig `json:"postgres"`
}

// read config file.
func readConfig(path string, config *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	err = json.Unmarshal(b, &config)
	if err != nil {
		return fmt.Errorf("could not parse json: %w", err)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("could not close file: %w", err)
	}

	return nil
}

const (
	DefaultPostgresPort = 5432
	DefaultWorkerCount  = 5
)

// initialize config with defaults.
func initConfig() (*Config, error) {
	config := Config{
		WorkerCount: DefaultWorkerCount,
		File:        "",
		Postgres: PostgresConfig{
			Host:     "localhost",
			Port:     DefaultPostgresPort,
			SSL:      false,
			Database: "postgres",
			User:     "postgres",
			Password: "",
		},
	}

	var workerCount, port int

	var confPath, host, user, password, db string

	var ssl bool

	flag.IntVar(&workerCount, "worker", config.WorkerCount, "worker count")
	flag.StringVar(&host, "host", config.Postgres.Host, "database host")
	flag.IntVar(&port, "port", config.Postgres.Port, "database port")
	flag.StringVar(&user, "user", config.Postgres.User, "database user")
	flag.StringVar(&password, "password", config.Postgres.Password, "database password")
	flag.StringVar(&db, "db", config.Postgres.Database, "database schema name")
	flag.BoolVar(&ssl, "ssl", config.Postgres.SSL, "database ssl mode")

	flag.StringVar(&config.File, "file", "", "csv file input path for query parameters")
	flag.StringVar(&confPath, "config", "", "custom config path")
	flag.Parse()

	// load user defined custom config file
	if confPath != "" {
		err := readConfig(confPath, &config)
		if err != nil {
			return nil, fmt.Errorf("invalid config %s, %w", confPath, err)
		}
	}

	// provided flags always override configuration
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "worker":
			config.WorkerCount = workerCount
		case "host":
			config.Postgres.Host = host
		case "port":
			config.Postgres.Port = port
		case "db":
			config.Postgres.Database = db
		case "user":
			config.Postgres.User = user
		case "password":
			config.Postgres.Password = password
		case "ssl":
			config.Postgres.SSL = ssl
		}
	})

	return &config, nil
}

func connectDB(host, user, password, dbname string, port int, ssl bool) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s",
		host, port, user, password, dbname,
	)
	if !ssl {
		dsn += " sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("can't open db connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("can't ping db: %w", err)
	}

	return db, nil
}

type QueryParams struct {
	Host  string
	Start time.Time
	End   time.Time
}

const testQuery = `
SELECT 
       time_bucket('1 minute', ts) AS one_min,
       AVG(usage),
       MIN(usage),
       MAX(usage)
FROM cpu_usage
WHERE host = $1
  AND ts BETWEEN $2 AND $3
GROUP BY one_min
ORDER BY one_min DESC;
`

func runTestQuery(db *sql.DB, q QueryParams) error {
	rows, err := db.Query(
		testQuery,
		q.Host,
		q.Start,
		q.End,
	)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	for rows.Next() {
		if rows.Err() != nil {
			return fmt.Errorf("failed to read rows: %w", err)
		}

		var oneMin time.Time

		var avg float64

		var max float64

		var min float64

		err = rows.Scan(&oneMin, &avg, &min, &max)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	defer func() {
		if err := rows.Close(); err != nil {
			return
		}
	}()

	return nil
}

type QueryResult struct {
	Duration float64
	Host     string
	Error    error
}

// initialize workers and setup channels.
func startWorkers(workerCount int, work func(q QueryParams) error) ([]chan QueryParams, chan QueryResult) {
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

const (
	DateTimeLayout = "2006-01-02 15:04:05"
	LineLength     = 3
)

var (
	errInvalidLineLen = errors.New("invalid line length")
	errEmptyHostname  = errors.New("empty host name")
	errInvalidHeader  = errors.New("invalid header")
)

func parseLine(line []string) (*QueryParams, error) {
	if len(line) != LineLength {
		return nil, fmt.Errorf("given line len: %d, %w", len(line), errInvalidLineLen)
	}

	hostname := line[0]
	if hostname == "" {
		return nil, errEmptyHostname
	}

	start, err := time.Parse(DateTimeLayout, line[1])
	if err != nil {
		return nil, fmt.Errorf("invalid start time value %v: %w", line[1], err)
	}

	end, err := time.Parse(DateTimeLayout, line[2])
	if err != nil {
		return nil, fmt.Errorf("invalid start time value %v: %w", line[2], err)
	}

	query := QueryParams{
		Host:  hostname,
		Start: start,
		End:   end,
	}

	return &query, nil
}

const ExpectedHeaderStr = "hostname,start_time,end_time"

func validateHeader(line []string) error {
	givenHeaderStr := strings.Join(line, ",")
	if givenHeaderStr == ExpectedHeaderStr {
		return nil
	}

	return fmt.Errorf("expected: %s but give: %s, %w", ExpectedHeaderStr, givenHeaderStr, errInvalidHeader)
}

func parseCsv(reader io.Reader, cb func(err error, params *QueryParams)) error {
	csvReader := csv.NewReader(reader)

	first, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("could not read header: %w", err)
	}

	err = validateHeader(first)
	if err != nil {
		return err
	}

	lineNum := 0

	for {
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		lineNum++

		if err != nil {
			cb(err, nil)

			continue
		}

		query, err := parseLine(record)
		if err != nil {
			cb(fmt.Errorf("record on line %d: %w", lineNum, err), nil)

			continue
		}

		cb(nil, query)
	}

	return nil
}

func readCsv(file string) (io.Reader, error) {
	if file == "" {
		return os.Stdin, nil
	}

	reader, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("could not open csv file: %w", err)
	}

	return reader, nil
}

// parse input rows and assign to workers.
func processCsv(file string, errCh chan error, workerChs []chan QueryParams) (parseFailure int, err error) {
	reader, err := readCsv(file)
	if err != nil {
		return
	}

	hostWorkerMap := map[string]int{}
	workerIndex := 0

	err = parseCsv(reader, func(err error, query *QueryParams) {
		if err != nil {
			errCh <- err
			parseFailure++

			return
		}

		w, ok := hostWorkerMap[query.Host]
		if !ok {
			w = workerIndex
			hostWorkerMap[query.Host] = w
			workerIndex = (workerIndex + 1) % len(workerChs)
		}

		ch := workerChs[w]
		ch <- *query
	})

	return
}

const (
	MaxPercentile        = 100
	MinPercentile        = 0
	MinPercentileDataLen = 2
)

func percentile(data []float64, p float64) float64 {
	if p < MinPercentile {
		return math.NaN()
	}

	if p > MaxPercentile {
		return math.NaN()
	}

	n := float64(len(data))

	if n < MinPercentileDataLen {
		return math.NaN()
	}

	rank := (p/100)*(n-1) + 1
	ri := float64(int64(rank))
	rf := rank - ri
	i := int(ri) - 1

	return data[i] + rf*(data[i+1]-data[i])
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
func collectResult(errCh chan error, result chan QueryResult, statCh chan Stats) {
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

func printResult(parseFailure int, stat Stats) {
	total := stat.ExecCount + stat.FailedCount + parseFailure
	fmt.Printf(
		resultMsg,
		total,
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

func main() {
	errLogger := log.New(os.Stderr, "", log.Lmsgprefix)

	config, err := initConfig()
	if err != nil {
		errLogger.Fatalln(err)
	}

	db, err := connectDB(
		config.Postgres.Host,
		config.Postgres.User,
		config.Postgres.Password,
		config.Postgres.Database,
		config.Postgres.Port,
		config.Postgres.SSL,
	)
	if err != nil {
		errLogger.Fatalln(err)
	}

	defer func() {
		if err = db.Close(); err != nil {
			errLogger.Println(err)
		}
	}()

	workerChs, result := startWorkers(config.WorkerCount, func(q QueryParams) error {
		return runTestQuery(db, q)
	})
	errCh := make(chan error)

	go func() {
		for err := range errCh {
			errLogger.Println(err)
		}
	}()

	statCh := make(chan Stats)
	go collectResult(errCh, result, statCh)

	parseFailure, err := processCsv(config.File, errCh, workerChs)
	if err != nil {
		errLogger.Fatalln(err)
	}

	for _, ch := range workerChs {
		close(ch)
	}

	stat := <-statCh

	close(errCh)

	printResult(parseFailure, stat)
}
