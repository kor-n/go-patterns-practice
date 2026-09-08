package shutdown

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}

func listen(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("не удалось открыть сокет: %v", err)
	}
	t.Cleanup(http.DefaultClient.CloseIdleConnections)

	return ln
}

func get(addr string) (int, string, error) {
	resp, err := http.Get(addr)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

func handler(fn http.HandlerFunc) *http.Server {
	return &http.Server{Handler: fn, ReadHeaderTimeout: time.Second}
}

func reply(text string) *http.Server {
	return handler(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, text)
	})
}

func launch(ctx context.Context, app App) <-chan error {
	done := make(chan error, 1)
	go func() { done <- Run(ctx, app) }()
	return done
}

func waitRun(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся")
		return nil
	}
}

func TestRunServesRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)
	addr := "http://" + ln.Addr().String()

	done := launch(ctx, App{Server: reply("ok"), Listener: ln, ShutdownTimeout: 3 * time.Second})

	code, body, err := get(addr)
	if err != nil {
		t.Fatalf("запрос не прошёл: %v", err)
	}
	if code != http.StatusOK || body != "ok" {
		t.Fatalf("получено %d %q, ожидалось 200 и ok", code, body)
	}

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run вернул %v, ожидался nil: http.ErrServerClosed это штатное завершение", err)
	}
}

func TestRunFinishesInFlightRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)
	addr := "http://" + ln.Addr().String()

	srv := handler(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, "done")
	})
	done := launch(ctx, App{Server: srv, Listener: ln, ShutdownTimeout: 3 * time.Second})

	type answer struct {
		code int
		body string
		err  error
	}
	got := make(chan answer, 1)
	go func() {
		code, body, err := get(addr)
		got <- answer{code, body, err}
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case a := <-got:
		if a.err != nil {
			t.Fatalf("принятый запрос оборвался: %v", a.err)
		}
		if a.code != http.StatusOK || a.body != "done" {
			t.Fatalf("получено %d %q, ожидалось 200 и done", a.code, a.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ответ на принятый запрос так и не пришёл")
	}

	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run вернул %v, ожидался nil", err)
	}
}

func TestRunStopsAcceptingRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)
	addr := "http://" + ln.Addr().String()

	done := launch(ctx, App{Server: reply("ok"), Listener: ln, ShutdownTimeout: 3 * time.Second})

	if _, _, err := get(addr); err != nil {
		t.Fatalf("запрос не прошёл до остановки: %v", err)
	}

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run вернул %v, ожидался nil", err)
	}

	if _, _, err := get(addr); err == nil {
		t.Fatal("сервер продолжает принимать запросы после остановки")
	}
}

func TestRunShutdownDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)
	addr := "http://" + ln.Addr().String()

	srv := handler(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { _ = srv.Close() })

	done := launch(ctx, App{Server: srv, Listener: ln, ShutdownTimeout: 100 * time.Millisecond})

	go func() { _, _, _ = get(addr) }()
	time.Sleep(100 * time.Millisecond)

	started := time.Now()
	cancel()
	err := waitRun(t, done)
	elapsed := time.Since(started)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("получена ошибка %v, ожидалась context.DeadlineExceeded", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Run ждал %v, ожидалось около 100ms: дедлайн остановки не соблюдён", elapsed)
	}
}

func TestRunWaitsForWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)

	var finished, seenOpen atomic.Bool
	var database *flagCloser
	worker := func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		seenOpen.Store(!database.closed.Load())
		finished.Store(true)
	}

	database = &flagCloser{}
	done := launch(ctx, App{
		Server:          reply("ok"),
		Listener:        ln,
		Worker:          worker,
		Deps:            []Closer{database},
		ShutdownTimeout: 3 * time.Second,
	})

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run вернул %v, ожидался nil", err)
	}
	if !finished.Load() {
		t.Fatal("Run вернулся, не дождавшись фонового воркера")
	}
	if !seenOpen.Load() {
		t.Fatal("база закрыта раньше, чем доработал фоновый воркер")
	}
}

type flagCloser struct {
	closed atomic.Bool
}

func (c *flagCloser) Close() error {
	c.closed.Store(true)
	return nil
}

func TestRunClosesDepsAfterInFlightRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)
	addr := "http://" + ln.Addr().String()

	database := &flagCloser{}
	srv := handler(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		if database.closed.Load() {
			fmt.Fprint(w, "closed")
			return
		}
		fmt.Fprint(w, "open")
	})

	done := launch(ctx, App{
		Server:          srv,
		Listener:        ln,
		Deps:            []Closer{database},
		ShutdownTimeout: 3 * time.Second,
	})

	got := make(chan string, 1)
	go func() {
		_, body, _ := get(addr)
		got <- body
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case body := <-got:
		if body != "open" {
			t.Fatal("база закрыта раньше, чем доработал уже принятый запрос")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ответ на принятый запрос так и не пришёл")
	}

	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run вернул %v, ожидался nil", err)
	}
	if !database.closed.Load() {
		t.Fatal("база так и не закрыта")
	}
}

type closer struct {
	name string
	log  *[]string
	mu   *sync.Mutex
	err  error
}

func (c *closer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	*c.log = append(*c.log, c.name)
	return c.err
}

var (
	errCache    = errors.New("cache failed")
	errDatabase = errors.New("database failed")
)

func TestRunClosesDepsInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln := listen(t)

	var (
		mu  sync.Mutex
		log []string
	)
	deps := []Closer{
		&closer{name: "cache", log: &log, mu: &mu, err: errCache},
		&closer{name: "database", log: &log, mu: &mu, err: errDatabase},
	}

	done := launch(ctx, App{
		Server:          reply("ok"),
		Listener:        ln,
		Deps:            deps,
		ShutdownTimeout: 3 * time.Second,
	})

	cancel()
	err := waitRun(t, done)

	mu.Lock()
	got := append([]string(nil), log...)
	mu.Unlock()

	want := []string{"cache", "database"}
	if len(got) != len(want) {
		t.Fatalf("закрыто %v, ожидалось %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("порядок закрытия %v, ожидался %v", got, want)
		}
	}
	if !errors.Is(err, errCache) {
		t.Fatalf("получена ошибка %v, в ней нет ошибки кэша", err)
	}
	if !errors.Is(err, errDatabase) {
		t.Fatalf("получена ошибка %v, в ней нет ошибки базы: "+
			"после ошибки на первой зависимости остальные всё равно надо закрыть", err)
	}
}

func TestRunReturnsServeError(t *testing.T) {
	ln := listen(t)
	if err := ln.Close(); err != nil {
		t.Fatalf("не удалось закрыть сокет: %v", err)
	}

	done := launch(t.Context(), App{
		Server:          reply("ok"),
		Listener:        ln,
		ShutdownTimeout: time.Second,
	})

	if err := waitRun(t, done); err == nil {
		t.Fatal("Run вернул nil, хотя сервер не смог обслуживать сокет")
	}
}
