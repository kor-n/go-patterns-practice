// Package workerpool: пул из N воркеров.
//
// Run читает задания из jobs, вызывает на каждом work и отдаёт результаты
// в выходной канал. RunFailFast прогоняет задания так же, но результаты не
// возвращает: от него нужна только ошибка, если какое-то задание упало.
//
// Требования к Run:
//   - воркеров ровно n, а не по одному на каждое задание, n <= 0 считается за 1;
//   - воркеры читают jobs, пока канал не закроется;
//   - в Result попадают номер задания, значение от work и ошибка;
//   - выходной канал закрывается, когда все воркеры закончили;
//   - паника внутри work становится ошибкой в Result.Err и не роняет пул;
//   - отмена контекста останавливает воркеров и закрывает выходной канал.
//
// Требования к RunFailFast:
//   - то же ограничение по числу воркеров;
//   - первая ошибка от work отменяет остальных и возвращается наружу,
//     иначе nil.
//
// Пример:
//
//	jobs := feed(1, 2, 3) // канал, отдающий 1, 2, 3
//
//	for r := range Run(ctx, jobs, 2, func(_ context.Context, j int) (int, error) {
//		return j * 10, nil
//	}) {
//		fmt.Println(r.Job, r.Val) // 1 10, 2 20, 3 30 в произвольном порядке
//	}
package workerpool

import "context"

// Result - результат одного задания.
type Result struct {
	Job int
	Val int
	Err error
}

// WorkFunc обрабатывает одно задание.
type WorkFunc func(ctx context.Context, job int) (int, error)

// Run разбирает задания из jobs пулом из n воркеров.
func Run(ctx context.Context, jobs <-chan int, n int, work WorkFunc) <-chan Result {
	panic("не реализовано")
}

// RunFailFast останавливает пул на первой ошибке и возвращает её.
func RunFailFast(ctx context.Context, jobs <-chan int, n int, work WorkFunc) error {
	panic("не реализовано")
}
