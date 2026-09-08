// Package debug: найти и починить ошибки в конкурентном коде.
//
// Три функции ниже уже написаны, но каждая содержит ошибку. Тесты падают.
// Найди причину и почини, не меняя сигнатуры. Тесты трогать нельзя.
//
// Ошибки разные, и ловят их разные инструменты: детектор гонок, таймаут
// в тесте и goleak. Смотри, чем именно упал прогон, это подсказка.
//
// Требования:
//   - Sum возвращает сумму всех чисел;
//   - Collect возвращает числа от 1 до n по порядку;
//   - Take возвращает первые n чисел из nums;
//   - go test -race проходит;
//   - после каждого теста не остаётся живых горутин.
//
// Пример:
//
//	Sum([]int{1, 2, 3})           // 6
//	Collect(3)                    // [1 2 3]
//	Take([]int{1, 2, 3, 4, 5}, 2) // [1 2]
package debug

import "sync"

// Sum складывает числа, считая их параллельно.
func Sum(nums []int) int {
	total := 0

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for _, n := range nums {
		go func() {
			defer wg.Done()
			total += n
		}()
	}
	wg.Wait()

	return total
}

// Collect возвращает числа от 1 до n.
func Collect(n int) []int {
	ch := make(chan int)
	go func() {
		for i := 1; i <= n; i++ {
			ch <- i
		}
	}()

	var res []int
	for v := range ch {
		res = append(res, v)
	}

	return res
}

// Take возвращает первые n чисел из nums.
func Take(nums []int, n int) []int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for _, v := range nums {
			ch <- v
		}
	}()

	res := make([]int, 0, n)
	for range n {
		res = append(res, <-ch)
	}

	return res
}
