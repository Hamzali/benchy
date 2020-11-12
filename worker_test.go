package benchy_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hamzali/benchy"
)

func TestStartWorkers(t *testing.T) {
	testErr := errors.New("test error")

	testParam := benchy.QueryParams{
		Host:  "test",
		Start: time.Now(),
		End:   time.Now(),
	}

	workerCount := 3

	expCallCount := 3
	callCount := 0

	workerChs, resCh := benchy.StartWorkers(workerCount, func(q benchy.QueryParams) error {
		if !reflect.DeepEqual(q, testParam) {
			t.Errorf("invalid param")
		}

		callCount++

		if callCount == 3 {
			return testErr
		}

		return nil
	})

	expResCount := 3
	expErrCount := 1

	errCount := 0
	resCount := 0

	resWg := sync.WaitGroup{}
	resWg.Add(1)

	go func() {
		for res := range resCh {
			if res.Error != nil {
				errCount++
			}
			resCount++
		}

		resWg.Done()
	}()

	for _, ch := range workerChs {
		ch <- testParam
		close(ch)
	}

	resWg.Wait()

	if expCallCount != callCount {
		t.Fatalf("expected %d calls but got %d", expCallCount, callCount)
	}

	if expResCount != resCount {
		t.Fatalf("expected %d results but got %d", expResCount, resCount)
	}

	if expErrCount != errCount {
		t.Fatalf("expected %d errors but got %d", expErrCount, errCount)
	}
}
