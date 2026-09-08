package syncpool

import (
	"fmt"
	"sync"
	"testing"
)

// sink удерживает результат в AllocsPerRun: там нет гарантий, что компилятор
// не выбросит вызов с неиспользованным результатом. В бенчмарках он не нужен,
// внутри for b.Loop() результаты вызовов удерживаются сами.
var sink string

func makeRecords(prefix string, n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{ID: i, Name: fmt.Sprintf("%s-rec-%d", prefix, i)}
	}
	return recs
}

// Со второго вызова буфер приходит из пула: без Reset в ответе будет хвост предыдущего.
func TestFormatPooledMatchesNaive(t *testing.T) {
	cases := [][]Record{
		nil,
		{},
		makeRecords("a", 1),
		makeRecords("b", 7),
		makeRecords("c", 100),
		makeRecords("d", 3),
	}

	for i, recs := range cases {
		want := FormatNaive(recs)
		got := FormatPooled(recs)
		if got != want {
			t.Fatalf("случай %d: получено %q, ожидалось %q", i, got, want)
		}
	}

	for i, recs := range cases {
		want := FormatNaive(recs)
		if got := FormatPooled(recs); got != want {
			t.Fatalf("повторный случай %d: получено %q, ожидалось %q, буфер не очищен после Get",
				i, got, want)
		}
	}
}

func TestFormatPooledFewerAllocs(t *testing.T) {
	recs := makeRecords("bench", 50)

	naive := testing.AllocsPerRun(200, func() { sink = FormatNaive(recs) })
	pooled := testing.AllocsPerRun(200, func() { sink = FormatPooled(recs) })

	t.Logf("аллокаций на вызов: naive %.1f, pooled %.1f", naive, pooled)

	if pooled >= naive {
		t.Fatalf("аллокаций на вызов: pooled %.1f, naive %.1f, пул ничего не сэкономил",
			pooled, naive)
	}
	if pooled > naive/2 {
		t.Fatalf("аллокаций на вызов: pooled %.1f, naive %.1f, буфер переиспользуется не полностью",
			pooled, naive)
	}
}

func TestFormatPooledIsConcurrencySafe(t *testing.T) {
	const goroutines = 50

	var wg sync.WaitGroup
	problems := make(chan string, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()

			recs := makeRecords(fmt.Sprintf("g%d", g), 20)
			want := FormatNaive(recs)

			for i := range 200 {
				if got := FormatPooled(recs); got != want {
					select {
					case problems <- fmt.Sprintf("горутина %d, итерация %d: получено %q, ожидалось %q",
						g, i, got, want):
					default:
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	close(problems)

	if msg, ok := <-problems; ok {
		t.Fatal(msg)
	}
}

func BenchmarkFormatNaive(b *testing.B) {
	recs := makeRecords("bench", 50)
	b.ReportAllocs()
	for b.Loop() {
		FormatNaive(recs)
	}
}

func BenchmarkFormatPooled(b *testing.B) {
	recs := makeRecords("bench", 50)
	b.ReportAllocs()
	for b.Loop() {
		FormatPooled(recs)
	}
}
