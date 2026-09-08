package fanin

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func closed(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func slowProducer(base, count int, d time.Duration) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			time.Sleep(d)
			ch <- base + i
		}
	}()
	return ch
}

func TestMergeCollectsAllValues(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	got := map[int]int{}
	total := 0
	for v := range Merge(ctx, closed(1, 2, 3), closed(4, 5), closed(6)) {
		got[v]++
		total++
	}

	if total != 6 {
		t.Fatalf("получено %d значений, ожидалось 6", total)
	}
	for _, v := range []int{1, 2, 3, 4, 5, 6} {
		if got[v] != 1 {
			t.Errorf("значение %d встретилось %d раз, ожидался 1", v, got[v])
		}
	}
}

func TestMergeClosesOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out := Merge(ctx, closed(1))
	<-out

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("выходной канал отдал лишнее значение")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("выходной канал не закрылся после закрытия всех входных")
	}
}

func TestMergeNoInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	select {
	case _, ok := <-Merge[int](ctx):
		if ok {
			t.Fatal("Merge без входов отдал значение")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Merge без входов должен вернуть закрытый канал")
	}
}

func TestMergeReadsChannelsConcurrently(t *testing.T) {
	const (
		delay = 100 * time.Millisecond
		count = 4
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	start := time.Now()
	out := Merge(ctx,
		slowProducer(10, count, delay),
		slowProducer(20, count, delay),
		slowProducer(30, count, delay),
	)

	n := 0
	for range out {
		n++
	}
	elapsed := time.Since(start)

	if n != 3*count {
		t.Fatalf("получено %d значений, ожидалось %d", n, 3*count)
	}
	if elapsed > 7*delay {
		t.Fatalf("заняло %v, ожидалось около %v", elapsed, count*delay)
	}
}

func TestMergeClosesWithStuckInput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	hung := make(chan int)
	out := Merge(ctx, (<-chan int)(hung), closed(42))

	select {
	case v := <-out:
		if v != 42 {
			t.Fatalf("получено %d, ожидалось 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("значение из живого канала не пришло: похоже, каналы читаются по очереди")
	}

	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("после отмены пришло лишнее значение")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("выходной канал не закрылся после отмены контекста")
	}
}

func TestMergeCancelReleasesGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	stop := make(chan struct{})
	defer close(stop)

	infinite := func() <-chan int {
		ch := make(chan int)
		go func() {
			for i := 0; ; i++ {
				select {
				case ch <- i:
				case <-stop:
					return
				}
			}
		}()
		return ch
	}

	out := Merge(ctx, infinite(), infinite(), infinite())

	for range 10 {
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
			t.Fatal("выходной канал не закрылся после отмены")
		}
	}
}

func TestMergeCancelWhenNobodyReads(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	stop := make(chan struct{})
	defer close(stop)

	infinite := func() <-chan int {
		ch := make(chan int)
		go func() {
			for i := 0; ; i++ {
				select {
				case ch <- i:
				case <-stop:
					return
				}
			}
		}()
		return ch
	}

	out := Merge(ctx, infinite(), infinite())

	<-out
	cancel()
	time.Sleep(200 * time.Millisecond)

	select {
	case v, ok := <-out:
		if ok {
			t.Fatalf("после отмены пришло значение %d: горутина висела на отправке "+
				"в out, значит отправка не обёрнута в select с ctx.Done()", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("выходной канал не закрылся после отмены")
	}
}

func TestMergeStrings(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mk := func(vals ...string) <-chan string {
		ch := make(chan string, len(vals))
		for _, v := range vals {
			ch <- v
		}
		close(ch)
		return ch
	}

	got := map[string]bool{}
	for v := range Merge(ctx, mk("a", "b"), mk("c")) {
		got[v] = true
	}

	for _, want := range []string{"a", "b", "c"} {
		if !got[want] {
			t.Errorf("значение %q потерялось", want)
		}
	}
}
