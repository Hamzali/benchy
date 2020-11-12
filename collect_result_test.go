package benchy_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/hamzali/benchy"
)

func TestCollectResult(t *testing.T) {
	testErr := errors.New("test error")

	testResults := []benchy.QueryResult{
		{Duration: 10, Host: "test_host", Error: nil},
		{Duration: 20, Host: "test_host", Error: nil},
		{Duration: 30, Host: "test_host", Error: nil},
		{Duration: 0, Host: "test_host", Error: testErr},
		{Duration: 0, Host: "test_host", Error: testErr},
	}

	expStat := benchy.Stats{
		ExecCount:      3,
		FailedCount:    2,
		Sum:            60,
		Max:            30,
		Min:            10,
		Median:         20,
		Mean:           20,
		Percentile95th: 29,
		Percentile99th: 29.8,
	}

	resCh := make(chan benchy.QueryResult)
	errCh := make(chan error)
	statCh := make(chan benchy.Stats)

	go benchy.CollectResult(errCh, resCh, statCh)

	go func() {
		for _, r := range testResults {
			resCh <- r
		}

		close(resCh)
	}()

	errMsgCount := 0
	errWg := sync.WaitGroup{}
	errWg.Add(1)

	go func() {
		for range errCh {
			errMsgCount++
		}

		errWg.Done()
	}()

	stat := <-statCh

	close(statCh)
	close(errCh)

	errWg.Wait()

	if errMsgCount != expStat.FailedCount {
		t.Fatalf("expected %d errors but got %d", expStat.FailedCount, errMsgCount)
	}

	if !reflect.DeepEqual(expStat, stat) {
		t.Fatalf("expected %v but got %v", expStat, stat)
	}
}
