package main

import (
	"log"
	"os"

	"github.com/hamzali/benchy"
	"github.com/hamzali/benchy/conf"
	"github.com/hamzali/benchy/database"
	_ "github.com/lib/pq"
)

func main() {
	errLogger := log.New(os.Stderr, "", log.Lmsgprefix)
	infoLogger := log.New(os.Stdout, "", log.Lmsgprefix)

	config, err := conf.InitConfig(os.Args[0], os.Args[1:])
	if err != nil {
		errLogger.Fatalln(err)
	}

	db, err := database.New(
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
	defer db.Close()

	workerChs, result := benchy.StartWorkers(config.WorkerCount, func(q benchy.QueryParams) error {
		return db.RunTestQuery(q.Host, q.Start, q.End)
	})
	errCh := make(chan error)

	go func() {
		for err := range errCh {
			errLogger.Println(err)
		}
	}()

	infoLogger.Println("workers started...")

	statCh := make(chan benchy.Stats)
	go benchy.CollectResult(errCh, result, statCh)

	reader, err := benchy.ReadCsv(config.File)
	if err != nil {
		errLogger.Fatalln(err)
	}

	parseFailure, err := benchy.ProcessCsv(reader, errCh, workerChs)
	if err != nil {
		errLogger.Fatalln(err)
	}

	for _, ch := range workerChs {
		close(ch)
	}

	stat := <-statCh

	close(errCh)

	resultStr := benchy.FormatStat(parseFailure, stat)

	infoLogger.Print(resultStr)
}
