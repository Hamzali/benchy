package conf

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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
func ReadConfig(path string, config *Config) error {
	if path == "" {
		return nil
	}

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
	defaultPostgresPort = 5432
	defaultWorkerCount  = 5
)

var DefaultConfig = Config{
	WorkerCount: defaultWorkerCount,
	File:        "",
	Postgres: PostgresConfig{
		Host:     "localhost",
		Port:     defaultPostgresPort,
		SSL:      false,
		Database: "postgres",
		User:     "postgres",
		Password: "",
	},
}

// initialize config with defaults.
func InitConfig(name string, args []string) (*Config, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	config := Config{}
	config = DefaultConfig

	var workerCount, port int

	var confPath, host, user, password, db string

	var ssl bool

	flags.IntVar(&workerCount, "worker", DefaultConfig.WorkerCount, "worker count")
	flags.StringVar(&host, "host", DefaultConfig.Postgres.Host, "database host")
	flags.IntVar(&port, "port", DefaultConfig.Postgres.Port, "database port")
	flags.StringVar(&user, "user", DefaultConfig.Postgres.User, "database user")
	flags.StringVar(&password, "password", DefaultConfig.Postgres.Password, "database password")
	flags.StringVar(&db, "db", DefaultConfig.Postgres.Database, "database schema name")
	flags.BoolVar(&ssl, "ssl", DefaultConfig.Postgres.SSL, "database ssl mode")

	flags.StringVar(&config.File, "file", "", "csv file input path for query parameters")
	flags.StringVar(&confPath, "config", "", "custom config path")

	err := flags.Parse(args)
	if err != nil {
		return nil, fmt.Errorf("flag error: %w", err)
	}

	// load user defined custom config file
	err = ReadConfig(confPath, &config)
	if err != nil {
		return nil, fmt.Errorf("invalid config %s, %w", confPath, err)
	}

	// provided flags always override configuration
	flags.Visit(func(f *flag.Flag) {
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
