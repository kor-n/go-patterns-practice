package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestGetReturnsFreshValue(t *testing.T) {
	c := New()
	defer c.Close()

	c.Set("k", "v", time.Minute)

	got, ok := c.Get("k")
	if !ok {
		t.Fatal("значение не найдено сразу после Set")
	}
	if got != "v" {
		t.Fatalf("получено %q, ожидалось %q", got, "v")
	}
}

func TestGetSkipsExpired(t *testing.T) {
	c := New()
	defer c.Close()

	c.Set("k", "v", 50*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Fatal("кэш отдал протухшее значение")
	}
}

func TestGetMissingKey(t *testing.T) {
	c := New()
	defer c.Close()

	if _, ok := c.Get("no-such-key"); ok {
		t.Fatal("кэш нашёл ключ, которого не клали")
	}
}

func TestExpiredEntriesAreEvicted(t *testing.T) {
	c := New()
	defer c.Close()

	for i := range 100 {
		c.Set(fmt.Sprintf("k%d", i), "v", 50*time.Millisecond)
	}
	if c.Len() == 0 {
		t.Fatal("Len вернул 0 сразу после записи 100 ключей")
	}

	time.Sleep(1500 * time.Millisecond)

	if n := c.Len(); n != 0 {
		t.Fatalf("через 1.5 секунды после истечения TTL в кэше осталось %d записей", n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := New()
	defer c.Close()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			for j := range 200 {
				c.Set(fmt.Sprintf("k%d", j%20), "v", time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := range 200 {
				c.Get(fmt.Sprintf("k%d", j%20))
				c.Len()
			}
		}()
	}

	wg.Wait()
}

func TestNewStartsBackgroundCleanup(t *testing.T) {
	before := runtime.NumGoroutine()

	c := New()
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("New не запустил фоновую горутину: без неё протухшие записи " +
		"вычищаются только при обращении к кэшу, а ключи, которые больше " +
		"никто не читает, остаются в памяти навсегда")
}

func TestCloseTwice(t *testing.T) {
	c := New()
	c.Close()
	c.Close()
}
