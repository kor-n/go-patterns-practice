package firstok

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

var errServer = errors.New("server unavailable")

func slow(d time.Duration, vals []string, err error, cancelled *atomic.Int64) func(context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		select {
		case <-time.After(d):
			return vals, err
		case <-ctx.Done():
			if cancelled != nil {
				cancelled.Add(1)
			}
			return nil, ctx.Err()
		}
	}
}

func TestSearchReturnsFirstSuccess(t *testing.T) {
	var cancelled atomic.Int64
	behaviour := map[string]func(context.Context) ([]string, error){
		"slow-1": slow(3*time.Second, []string{"поздно"}, nil, &cancelled),
		"fast":   slow(50*time.Millisecond, []string{"вовремя"}, nil, nil),
		"slow-2": slow(3*time.Second, nil, errServer, &cancelled),
	}

	start := time.Now()
	got, err := Search(t.Context(), []string{"slow-1", "fast", "slow-2"}, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			return behaviour[server](ctx)
		})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("вернулась ошибка: %v", err)
	}
	if len(got) != 1 || got[0] != "вовремя" {
		t.Fatalf("получено %v, ожидалось [вовремя]", got)
	}
	if elapsed > time.Second {
		t.Fatalf("ждали %v, ожидалось меньше секунды", elapsed)
	}

	time.Sleep(200 * time.Millisecond)
	if cancelled.Load() == 0 {
		t.Error("медленные серверы не получили отмену после первого успеха")
	}
}

func TestSearchIgnoresPartialErrors(t *testing.T) {
	behaviour := map[string]func(context.Context) ([]string, error){
		"bad-1": slow(10*time.Millisecond, nil, errServer, nil),
		"bad-2": slow(20*time.Millisecond, nil, errServer, nil),
		"good":  slow(80*time.Millisecond, []string{"ответ"}, nil, nil),
	}

	got, err := Search(t.Context(), []string{"bad-1", "bad-2", "good"}, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			return behaviour[server](ctx)
		})
	if err != nil {
		t.Fatalf("вернул ошибку, хотя один сервер ответил успешно: %v", err)
	}
	if len(got) != 1 || got[0] != "ответ" {
		t.Fatalf("получено %v, ожидалось [ответ]", got)
	}
}

func TestSearchErrorWhenAllFail(t *testing.T) {
	_, err := Search(t.Context(), []string{"a", "b", "c"}, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			return nil, errServer
		})

	if err == nil {
		t.Fatal("все серверы упали, а ошибки нет")
	}
}

func TestSearchRespectsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := Search(ctx, []string{"a", "b", "c"}, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			return slow(5*time.Second, []string{"поздно"}, nil, nil)(ctx)
		})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("дедлайн истёк, а ошибки нет")
	}
	if elapsed > time.Second {
		t.Fatalf("вернулся через %v, ожидалось около 50ms", elapsed)
	}
}

func TestSearchEmptyServerList(t *testing.T) {
	_, err := Search(t.Context(), nil, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			t.Fatal("search не должен вызываться на пустом списке")
			return nil, nil
		})

	if err == nil {
		t.Fatal("на пустом списке серверов ожидалась ошибка")
	}
}

func TestSearchQueriesEachServerOnce(t *testing.T) {
	var calls atomic.Int64

	_, _ = Search(t.Context(), []string{"a", "b", "c", "d"}, "q",
		func(ctx context.Context, server, query string) ([]string, error) {
			calls.Add(1)
			return nil, errServer
		})

	if got := calls.Load(); got != 4 {
		t.Fatalf("search вызван %d раз, ожидалось 4", got)
	}
}
