package benchy_test

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hamzali/benchy"
)

func TestProcessCsv(t *testing.T) {
	const testCsvContent = `hostname,start_time,end_time
host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000002,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000003,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000004,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000004,2017-01-01 08:59:22,2017-01-01 09:59:22
,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000008,2017-01-01:22,2017-01-01 09:59:22
host_000008,2017-01-01:22,2017-01-01 09:59:22,wrong_line
host_000008,2017-01-01 08:59:22,2017-01-01 09:59`

	expParseErr := 4

	chCount := 3

	errCh := make(chan error)
	queryChs := []chan benchy.QueryParams{}

	for i := 0; i < chCount; i++ {
		queryChs = append(queryChs, make(chan benchy.QueryParams))
	}

	var wg sync.WaitGroup

	wg.Add(len(queryChs) + 1)

	go func() {
		defer wg.Done()

		c := 0

		for err := range errCh {
			if err != nil {
				c++
			}
		}

		if c != expParseErr {
			t.Errorf("not all %d errors are revcieved on err channel", expParseErr)
		}
	}()

	expChHosts := []map[string]int{
		{"host_000001": 3, "host_000004": 2},
		{"host_000002": 1},
		{"host_000003": 1},
	}

	for i, expHosts := range expChHosts {
		go func(index int, hosts map[string]int) {
			defer wg.Done()

			th := map[string]int{}

			for h := range hosts {
				th[h] = 0
			}

			for q := range queryChs[index] {
				if _, ok := hosts[q.Host]; !ok {
					t.Error("received expected host name")
				} else {
					th[q.Host]++
				}
			}

			if !reflect.DeepEqual(hosts, th) {
				t.Errorf("sent param count does not match expected %v but got %v", hosts, th)
			}
		}(i, expHosts)
	}

	r := strings.NewReader(testCsvContent)
	parseErr, err := benchy.ProcessCsv(r, errCh, queryChs)

	close(errCh)

	for _, qCh := range queryChs {
		close(qCh)
	}

	wg.Wait()

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if parseErr != expParseErr {
		t.Fatalf("expected %d parse errors but received %d", expParseErr, parseErr)
	}
}
