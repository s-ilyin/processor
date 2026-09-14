package processor

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// Option — функциональная опция для NewPeriodTask.
type Option func(*options)

type options struct {
	registry prometheus.Registerer
	logger   *slog.Logger

	onStart func(ctx context.Context)
	onStop  func(ctx context.Context)
	onError func(ctx context.Context, err error)
	onPanic func(ctx context.Context, val any)
}

func defaultOptions() options {
	return options{
		registry: nil, // nil = prometheus.DefaultRegisterer (см. newMetrics)
		logger:   slog.Default(),
	}
}

// WithPromRegistry задаёт prometheus.Registerer, в который будут
// зарегистрированы метрики процессора. Если не задан — используется
// prometheus.DefaultRegisterer. Повторная регистрация тех же самых
// коллекторов в одном реестре безопасна (несколько Processor-ов
// могут делить один реестр).
func WithPromRegistry(reg prometheus.Registerer) Option {
	return func(o *options) {
		o.registry = reg
	}
}

// WithLogger задаёт логгер. По умолчанию slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithOnStart задаёт колбэк, вызываемый после запуска event loop.
// Хук должен быть неблокирующим.
func WithOnStart(fn func(ctx context.Context)) Option {
	return func(o *options) { o.onStart = fn }
}

// WithOnStop задаёт колбэк, вызываемый после остановки всех задач
// и завершения event loop. Хук должен быть неблокирующим.
func WithOnStop(fn func(ctx context.Context)) Option {
	return func(o *options) { o.onStop = fn }
}

// WithOnError задаёт колбэк, вызываемый при исчерпании всех попыток
// (последняя попытка тоже вернула ошибку). Хук должен быть неблокирующим.
func WithOnError(fn func(ctx context.Context, err error)) Option {
	return func(o *options) { o.onError = fn }
}

// WithOnPanic задаёт колбэк, вызываемый при панике в хендлере.
// Хук должен быть неблокирующим.
func WithOnPanic(fn func(ctx context.Context, panicVal any)) Option {
	return func(o *options) { o.onPanic = fn }
}
