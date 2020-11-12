package benchy_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hamzali/benchy"
)

func TestReadCsv(t *testing.T) {
	t.Run("should return stdin with empty file name", func(st *testing.T) {
		reader, err := benchy.ReadCsv("")
		if err != nil {
			st.Fatal(err)
		}

		if reader != os.Stdin {
			st.Fatalf("did not return stdin")
		}
	})

	t.Run("should return file reader with valid file path", func(st *testing.T) {
		reader, err := benchy.ReadCsv("./test_data/query_params.csv")
		if err != nil {
			st.Fatal(err)
		}
		if reader == nil {
			st.Fatalf("did not return any reader")
		}
	})

	t.Run("should return error for invalid file path", func(st *testing.T) {
		reader, err := benchy.ReadCsv("/invalid/file/path")
		if err == nil {
			st.Fatal("did not return error")
		}
		if reader != nil {
			st.Fatalf("did return some reader")
		}
	})
}

func TestParseCsv(t *testing.T) {
	const testCsvContent = `hostname,start_time,end_time
host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000008,2017-01-01:22,2017-01-01 09:59:22
host_000008,2017-01-01:22,2017-01-01 09:59:22,wrong_line
host_000008,2017-01-01 08:59:22,2017-01-01 09:59`

	t.Run("should fail for invalid header", func(st *testing.T) {
		r := strings.NewReader("invalid,header,value\nhost_000008,2017-01-01 08:59:22,2017-01-01 09:59:22")
		err := benchy.ParseCsv(r, func(err error, host string, start, end time.Time) {})
		if !errors.Is(err, benchy.ErrInvalidHeader) {
			st.Fatalf("expected %v but got %v error", benchy.ErrInvalidHeader, err)
		}
	})

	t.Run("should fail for empty header line", func(st *testing.T) {
		r := strings.NewReader("")
		err := benchy.ParseCsv(r, func(err error, host string, start, end time.Time) {})
		if err == nil {
			st.Fatal("expected error but got nil")
		}
	})

	t.Run("should parse and provide line errors", func(st *testing.T) {
		r := strings.NewReader(testCsvContent)

		errs := []error{}

		err := benchy.ParseCsv(r, func(err error, host string, start, end time.Time) {
			if err != nil {
				errs = append(errs, err)

				return
			}

			if host != "host_000008" {
				st.Fatalf("expected host 'host_000008' but got '%s'", host)
			}

			if start.Format(benchy.DateTimeLayout) != "2017-01-01 08:59:22" {
				st.Fatal("start invalid")
			}

			if end.Format(benchy.DateTimeLayout) != "2017-01-01 09:59:22" {
				st.Fatal("end invalid")
			}
		})
		if err != nil {
			st.Fatal(err)
		}

		if len(errs) != 4 {
			st.Fatal("invalid parse errors")
		}
	})
}
