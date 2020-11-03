package benchy

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const (
	DateTimeLayout = "2006-01-02 15:04:05"
	lineLength     = 3
)

var (
	ErrInvalidLineLen = errors.New("invalid line length, must be equal to 3")
	ErrEmptyHostname  = errors.New("empty host name")
	ErrInvalidHeader  = errors.New("invalid header")
)

func parseLine(line []string) (hostname string, start, end time.Time, err error) {
	if len(line) != lineLength {
		err = ErrInvalidLineLen

		return
	}

	hostname = line[0]
	if hostname == "" {
		err = ErrEmptyHostname

		return
	}

	start, err = time.Parse(DateTimeLayout, line[1])
	if err != nil {
		err = fmt.Errorf("invalid start time value %v: %w", line[1], err)

		return
	}

	end, err = time.Parse(DateTimeLayout, line[2])
	if err != nil {
		err = fmt.Errorf("invalid start time value %v: %w", line[2], err)

		return
	}

	return
}

const expectedHeaderStr = "hostname,start_time,end_time"

func validateHeader(line []string) error {
	givenHeaderStr := strings.Join(line, ",")
	if givenHeaderStr == expectedHeaderStr {
		return nil
	}

	return ErrInvalidHeader
}

func ParseCsv(reader io.Reader, cb func(err error, host string, start, end time.Time)) error {
	csvReader := csv.NewReader(reader)

	first, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("could not read header: %w", err)
	}

	err = validateHeader(first)
	if err != nil {
		return err
	}

	lineNum := 1

	for {
		var host string

		var start, end time.Time

		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		lineNum++

		if err != nil {
			cb(err, host, start, end)

			continue
		}

		host, start, end, err = parseLine(record)
		if err != nil {
			cb(
				fmt.Errorf("record on line %d: %w", lineNum, err),
				host, start, end,
			)

			continue
		}

		cb(nil, host, start, end)
	}

	return nil
}

func ReadCsv(file string) (io.Reader, error) {
	if file == "" {
		return os.Stdin, nil
	}

	reader, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("could not open csv file: %w", err)
	}

	return reader, nil
}
