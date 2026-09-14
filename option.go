package processor

import "context"

// OnlyErrorWait преобразует хендлер, возвращающий только error, в формат
// планировщика.
//
// Поведение wait:
//   - f вернула ошибку → wait = true  (ждём Interval перед следующей попыткой);
//   - f завершилась успешно → wait = false (следующая задача запускается сразу).
//
// Подходит для воркеров, которые не знают о наличии работы, но хотят
// сделать паузу при ошибке (например, при недоступности внешнего API).
func OnlyErrorWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		err := f(ctx)
		if err != nil {
			return true, err
		}

		return false, nil
	}
}

// AlwaysWait преобразует хендлер, возвращающий только error, в формат
// планировщика, всегда устанавливая wait = true.
//
// Поведение wait:
//   - любое завершение f (успех или ошибка) → wait = true
//     (следующая попытка строго через Interval).
//
// Подходит для регулярных задач по расписанию, где важно соблюдать
// фиксированный интервал, а не запускать следующую итерацию сразу.
func AlwaysWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return true, f(ctx)
	}
}

// SkipWait преобразует хендлер, возвращающий только error, в формат
// планировщика, всегда устанавливая wait = false.
//
// Поведение wait:
//   - любое завершение f (успех или ошибка) → wait = false
//     (следующая задача запускается сразу после завершения текущей).
//
// Подходит для непрерывных воркеров без пауз (например, «читаем из
// канала и обрабатываем»). Осторожно: при постоянных ошибках приведёт
// к busy-loop — комбинируйте с MaxAttempts или собственным backoff.
func SkipWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return false, f(ctx)
	}
}

// WaitOnIdle оборачивает хендлер, который сам сообщает, была ли
// обработана работа (processed bool).
//
// Поведение wait:
//   - f вернула ошибку                 → wait = true  (ждём Interval);
//   - f вернула processed = false, nil → wait = true  (работы не было, ждём Interval);
//   - f вернула processed = true, nil  → wait = false (сразу берём следующую).
//
// Ошибка имеет приоритет над processed: если f вернула (true, err),
// wait всё равно будет true, потому что при ошибке нужно подождать перед
// следующей попыткой.
//
// Подходит для воркеров очередей: пустая очередь не крутит цикл вхолостую,
// а непустая обрабатывается без задержек.
func WaitOnIdle(f func(ctx context.Context) (processed bool, err error)) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		processed, err := f(ctx)
		if err != nil {
			return true, err
		}
		return !processed, nil
	}
}
