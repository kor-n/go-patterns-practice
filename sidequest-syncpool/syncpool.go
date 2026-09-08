// Package syncpool: переиспользование буферов через sync.Pool.
//
// FormatNaive собирает из записей одну строку и на каждый вызов создаёт
// новый буфер. Менять его не нужно, это эталон поведения. FormatPooled
// должен давать тот же результат, но брать буферы из пула.
//
// Требования:
//   - вывод совпадает с FormatNaive на любом входе;
//   - аллокаций на вызов меньше, проверяется через testing.AllocsPerRun;
//   - можно вызывать из нескольких горутин одновременно;
//   - буфер надо очищать перед использованием, иначе в ответ попадут куски
//     предыдущих вызовов.
//
// Когда тесты пройдут, прогнать бенчмарк и посмотреть на B/op и allocs/op:
//
//	go test ./sidequest-syncpool -bench . -benchmem
//
// Пример:
//
//	FormatPooled([]Record{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}})
//	// "id=1,name=a;id=2,name=b"
package syncpool

import (
	"bytes"
	"strconv"
)

// Record - запись, которую надо отформатировать.
type Record struct {
	ID   int
	Name string
}

func write(b *bytes.Buffer, recs []Record) {
	for i, r := range recs {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString("id=")
		b.WriteString(strconv.Itoa(r.ID))
		b.WriteString(",name=")
		b.WriteString(r.Name)
	}
}

// FormatNaive создаёт новый буфер на каждый вызов. Менять не нужно.
func FormatNaive(recs []Record) string {
	var b bytes.Buffer
	write(&b, recs)
	return b.String()
}

// FormatPooled делает то же, что FormatNaive, но переиспользует буферы.
func FormatPooled(recs []Record) string {
	panic("не реализовано")
}
