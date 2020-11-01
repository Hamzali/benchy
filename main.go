package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strings"
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

const DateTimeLayout = "2006-01-02 15:04:05"

func parseLine(line []string) (*QueryParams, error) {
	if len(line) != 3 {
		return nil, fmt.Errorf("invalid line length %d", len(line))
	}

	hostname := line[0]
	if hostname == "" {
		return nil, fmt.Errorf("empty host name")
	}

	start, err := time.Parse(DateTimeLayout, line[1])
	if err != nil {
		return nil, fmt.Errorf("invalid start time value %v", line[1])
	}

	end, err := time.Parse(DateTimeLayout, line[2])
	if err != nil {
		return nil, fmt.Errorf("invalid end time value %v", line[2])
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
	return fmt.Errorf("invalid header, expected: %s but give: %s", ExpectedHeaderStr, givenHeaderStr)
}

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

func readConfig(path string, config *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &config)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	return nil
}

func initConfig() (*Config, error) {
	config := Config{
		WorkerCount: 5,
		Postgres: PostgresConfig{
			Host:     "localhost",
			Port:     5432,
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
			return nil, fmt.Errorf("invalid config %s, %v", confPath, err)
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

func connectDB(conf PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s",
		conf.Host, conf.Port, conf.User, conf.Password, conf.Database,
	)
	if !conf.SSL {
		dsn += " sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func percentile(data []float64, p float64) float64 {
	if p < 0 {
		return math.NaN()
	}
	if p > 100 {
		return math.NaN()
	}
	n := float64(len(data))
	if n < 2 {
		return math.NaN()
	}
	rank := (p/100)*(n-1) + 1
	ri := float64(int64(rank))
	rf := rank - ri
	i := int(ri) - 1
	return data[i] + rf*(data[i+1]-data[i])
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

const LoggerPrefix = ""

func main() {
	errLogger := log.New(os.Stderr, LoggerPrefix, log.Lmsgprefix)
	infoLogger := log.New(os.Stdout, LoggerPrefix, log.Lmsgprefix)

	// read and parse user flags
	config, err := initConfig()
	if err != nil {
		errLogger.Fatalln(err)
	}

	// initialize workers
	var workerChs []chan QueryParams
	result := make(chan QueryResult)
	var workerWg sync.WaitGroup
	workerWg.Add(config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		workerChs = append(workerChs, make(chan QueryParams))
		db, err := connectDB(config.Postgres)
		if err != nil {
			errLogger.Fatalln(err)
		}
		go func(ch chan QueryParams) {
			defer workerWg.Done()
			for q := range ch {
				now := time.Now()
				_, err := db.Exec(
					testQuery,
					q.Host,
					q.Start,
					q.End,
				)
				elapsed := time.Since(now)
				r := QueryResult{
					Host:     q.Host,
					Duration: float64(elapsed.Milliseconds()),
					Error:    err,
				}
				result <- r
			}
			err = db.Close()
			if err != nil {
				errLogger.Println(err)
			}
		}(workerChs[i])
	}
	if workerChs == nil || len(workerChs) != config.WorkerCount {
		errLogger.Fatalln("worker setup failure")
	}

	// close result channel after every worker finishes
	go func() {
		workerWg.Wait()
		close(result)
	}()

	// listen to results and accumulate then print
	var resWg sync.WaitGroup
	resWg.Add(1)
	parseFailure := 0
	go func() {
		var durations []float64
		execCount := 0
		sqlFailure := 0

		min := math.Inf(1)
		max := math.Inf(-1)
		var sum float64 = 0
		for r := range result {
			if r.Error != nil {
				errLogger.Printf("sql err: %v\n", r.Error)
				sqlFailure += 1
				continue
			}

			execCount += 1
			durations = append(durations, r.Duration)
			min = math.Min(min, r.Duration)
			max = math.Max(max, r.Duration)
			sum += r.Duration
		}

		mean := sum / float64(execCount)
		totalCount := execCount + sqlFailure + parseFailure
		sort.Float64s(durations)
		infoLogger.Printf(
			resultMsg,
			totalCount,
			execCount,
			sqlFailure,
			parseFailure,
			sum,
			min,
			max,
			mean,
			percentile(durations, 50), // median
			percentile(durations, 95),
			percentile(durations, 99),
		)
		resWg.Done()
	}()

	// parse input rows and assign to workers
	var reader io.Reader = os.Stdin
	if config.File != "" {
		f, err := os.Open(config.File)
		if err != nil {
			errLogger.Fatalln(err)
		}
		reader = f
	}
	csvReader := csv.NewReader(reader)
	first, err := csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}
	err = validateHeader(first)
	if err != nil {
		errLogger.Fatalln(err)
	}
	lineNum := 2
	hostWorkerMap := map[string]int{}
	workerIndex := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			errLogger.Println(err)
			parseFailure += 1
			continue
		}

		query, err := parseLine(record)
		if err != nil {
			errLogger.Printf("record on line %d: %v\n", lineNum, err)
			parseFailure += 1
			continue
		}

		lineNum += 1

		w, ok := hostWorkerMap[query.Host]
		if !ok {
			w = workerIndex
			hostWorkerMap[query.Host] = w
			workerIndex = (workerIndex + 1) % config.WorkerCount
		}

		ch := workerChs[w]
		ch <- *query
	}

	for _, ch := range workerChs {
		close(ch)
	}

	resWg.Wait()
}
