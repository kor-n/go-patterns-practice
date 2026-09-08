package debug

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSum(t *testing.T) {
	nums := make([]int, 200)
	want := 0
	for i := range nums {
		nums[i] = i
		want += i
	}

	if got := Sum(nums); got != want {
		t.Fatalf("Sum вернул %d, ожидалось %d", got, want)
	}
}

func TestSumEmpty(t *testing.T) {
	if got := Sum(nil); got != 0 {
		t.Fatalf("Sum вернул %d, ожидался 0", got)
	}
}

func TestCollect(t *testing.T) {
	got := make(chan []int, 1)
	go func() { got <- Collect(5) }()

	select {
	case res := <-got:
		if want := []int{1, 2, 3, 4, 5}; !equal(res, want) {
			t.Fatalf("Collect вернул %v, ожидалось %v", res, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Collect не вернулся: чтение из канала не заканчивается")
	}
}

func TestCollectZero(t *testing.T) {
	got := make(chan []int, 1)
	go func() { got <- Collect(0) }()

	select {
	case res := <-got:
		if len(res) != 0 {
			t.Fatalf("Collect(0) вернул %v, ожидался пустой слайс", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Collect(0) не вернулся")
	}
}

func TestTake(t *testing.T) {
	defer goleak.VerifyNone(t)

	got := Take([]int{1, 2, 3, 4, 5}, 2)
	if want := []int{1, 2}; !equal(got, want) {
		t.Fatalf("Take вернул %v, ожидалось %v", got, want)
	}
}

func TestTakeAll(t *testing.T) {
	defer goleak.VerifyNone(t)

	nums := []int{1, 2, 3}
	got := Take(nums, len(nums))
	if !equal(got, nums) {
		t.Fatalf("Take вернул %v, ожидалось %v", got, nums)
	}
}
