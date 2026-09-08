// Package urlcheck: проверка доступности списка URL.
//
// CheckSeq и CheckPar делают HTTP GET на каждый URL из urls и возвращают
// по одному результату на каждый: сам URL и признак доступности.
//
// Требования:
//   - URL доступен, если ответ пришёл с кодом 200;
//   - если запрос вернул ошибку или другой код, URL недоступен;
//   - CheckSeq обходит URL последовательно;
//   - CheckPar запускает горутину на каждый URL;
//   - обе возвращают результаты в порядке входного слайса;
//   - тело ответа надо закрывать, иначе утекут соединения.
//
// Пример:
//
//	CheckSeq([]string{"https://example.com", "https://example.com/404"})
//	// [{https://example.com true} {https://example.com/404 false}]
package urlcheck

// Result - результат проверки одного URL.
type Result struct {
	URL string
	OK  bool
}

// CheckSeq проверяет URL последовательно.
func CheckSeq(urls []string) []Result {
	panic("не реализовано")
}

// CheckPar проверяет URL параллельно, сохраняя порядок результатов.
func CheckPar(urls []string) []Result {
	panic("не реализовано")
}
