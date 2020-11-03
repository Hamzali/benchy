package database

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrDBNotInitialized = errors.New("database connection not initialized")

type Database struct {
	conn *sql.DB
}

func New(host, user, password, dbname string, port int, ssl bool) (*Database, error) {
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

	return &Database{conn: db}, nil
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

func (db *Database) RunTestQuery(host string, start, end time.Time) error {
	if db.conn == nil {
		return ErrDBNotInitialized
	}

	rows, err := db.conn.Query(
		testQuery,
		host,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	for rows.Next() {
		if rows.Err() != nil {
			return fmt.Errorf("failed to read rows: %w", err)
		}

		var oneMin time.Time

		var avg, max, min float64

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

func (db *Database) Close() {
	if db.conn == nil {
		return
	}

	if err := db.conn.Close(); err != nil {
		return
	}
}
