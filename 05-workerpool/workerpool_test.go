package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func feed(ctx context.Context, n int) <-chan int {
	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for i := 1; i <= n; i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return jobs
}

func echo(_ context.Context, job int) (int, error) { return job, nil }

func TestRunProcessesAllJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const n = 100
	got := map[int]int{}
	for r := range Run(ctx, feed(ctx, n), 4, func(_ context.Context, job int) (int, error) {
		return job * 2, nil
	}) {
		if r.Err != nil {
			t.Fatalf("задание %d вернуло ошибку: %v", r.Job, r.Err)
		}
		got[r.Val]++
	}

	if len(got) != n {
		t.Fatalf("получено %d разных результатов, ожидалось %d", len(got), n)
	}
	for i := 1; i <= n; i++ {
		if got[i*2] != 1 {
			t.Fatalf("результат задания %d встретился %d раз, ожидался 1", i, got[i*2])
		}
	}
}

func TestRunClosesOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out := Run(ctx, feed(ctx, 3), 2, echo)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range out {
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("выходной канал не закрылся после того, как кончились задания")
	}
}

func TestRunUsesExactlyNWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const workers = 3

	var now, peak atomic.Int64
	work := func(_ context.Context, job int) (int, error) {
		cur := now.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		now.Add(-1)
		return job, nil
	}

	for range Run(ctx, feed(ctx, 30), workers, work) {
	}

	if got := peak.Load(); got > workers {
		t.Fatalf("одновременно работало %d воркеров, ожидалось не больше %d", got, workers)
	}
	if got := peak.Load(); got < workers {
		t.Fatalf("одновременно работало максимум %d воркеров, ожидалось %d", got, workers)
	}
}

func TestRunPanicDoesNotKillPool(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const n = 10
	var panicked, ok int
	for r := range Run(ctx, feed(ctx, n), 1, func(_ context.Context, job int) (int, error) {
		if job == 5 {
			panic("задание развалилось")
		}
		return job, nil
	}) {
		switch {
		case r.Job == 5 && r.Err != nil:
			panicked++
		case r.Err == nil:
			ok++
		default:
			t.Errorf("задание %d вернуло ошибку: %v", r.Job, r.Err)
		}
	}

	if panicked != 1 {
		t.Errorf("паника превратилась в ошибку %d раз, ожидался 1", panicked)
	}
	if ok != n-1 {
		t.Errorf("успешно обработано %d заданий, ожидалось %d", ok, n-1)
	}
}

func TestRunCancelStopsPool(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out := Run(ctx, feed(ctx, 1000), 4, func(ctx context.Context, job int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return job, nil
	})

	for range 5 {
		<-out
	}
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("выходной канал не закрылся после отмены контекста")
		}
	}
}

func TestRunCancelStopsPoolWithLiveProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	stop := make(chan struct{})
	defer close(stop)

	jobs := make(chan int)
	go func() {
		for i := 1; ; i++ {
			select {
			case jobs <- i:
			case <-stop:
				return
			}
		}
	}()

	out := Run(ctx, jobs, 4, func(_ context.Context, job int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return job, nil
	})

	for range 5 {
		<-out
	}
	cancel()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("выходной канал не закрылся после отмены: воркеры не смотрят на ctx")
		}
	}
}

func TestRunFailFastUsesExactlyNWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const workers = 3

	var now, peak atomic.Int64
	err := RunFailFast(ctx, feed(ctx, 30), workers, func(_ context.Context, job int) (int, error) {
		cur := now.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		now.Add(-1)
		return job, nil
	})
	if err != nil {
		t.Fatalf("вернулась ошибка: %v", err)
	}

	if got := peak.Load(); got > workers {
		t.Fatalf("одновременно работало %d воркеров, ожидалось не больше %d", got, workers)
	}
	if got := peak.Load(); got < workers {
		t.Fatalf("одновременно работало максимум %d воркеров, ожидалось %d", got, workers)
	}
}

func TestRunZeroWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	n := 0
	for range Run(ctx, feed(ctx, 5), 0, echo) {
		n++
	}

	if n != 5 {
		t.Fatalf("при n=0 обработано %d заданий, ожидалось 5", n)
	}
}

var errJob = errors.New("job failed")

func TestRunFailFastReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := RunFailFast(ctx, feed(ctx, 100), 4, func(ctx context.Context, job int) (int, error) {
		if job == 3 {
			return 0, errJob
		}
		select {
		case <-time.After(2 * time.Second):
			return job, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	if !errors.Is(err, errJob) {
		t.Fatalf("получена ошибка %v, ожидалась %v", err, errJob)
	}
}

func TestRunFailFastCancelsRest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var cancelled atomic.Int64

	start := time.Now()
	_ = RunFailFast(ctx, feed(ctx, 100), 8, func(ctx context.Context, job int) (int, error) {
		if job == 1 {
			time.Sleep(20 * time.Millisecond)
			return 0, errJob
		}
		select {
		case <-time.After(3 * time.Second):
			return job, nil
		case <-ctx.Done():
			cancelled.Add(1)
			return 0, ctx.Err()
		}
	})
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("вернулся через %v, остальные задания не отменены", elapsed)
	}
	if cancelled.Load() == 0 {
		t.Fatal("ни одно из оставшихся заданий не получило отмену через ctx")
	}
}

func TestRunFailFastNoErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := RunFailFast(ctx, feed(ctx, 50), 4, echo); err != nil {
		t.Fatalf("ожидался nil, получено %v", err)
	}
}
