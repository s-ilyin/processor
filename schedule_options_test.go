package processor

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestDefaultOptions проверяет дефолтные значения: nil registry
// (значит DefaultRegisterer в newMetrics), slog.Default() и nil-хуки.
func TestDefaultOptions(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	require.Nil(t, o.registry, "default registry must be nil (→ DefaultRegisterer)")
	require.NotNil(t, o.logger, "default logger must not be nil")
	require.Nil(t, o.onStart)
	require.Nil(t, o.onStop)
	require.Nil(t, o.onError)
	require.Nil(t, o.onPanic)
}

// TestWithPromRegistry проверяет, что опция кладёт реестр в options.
func TestWithPromRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	o := defaultOptions()
	WithPromRegistry(reg)(&o)
	require.Equal(t, reg, o.registry)
}

// TestWithLogger проверяет, что опция переопределяет логгер.
func TestWithLogger(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(nil, nil))
	o := defaultOptions()
	WithLogger(logger)(&o)
	require.Same(t, logger, o.logger)
}

// TestWithOnStart проверяет, что колбэк сохраняется и вызывается.
func TestWithOnStart(t *testing.T) {
	t.Parallel()

	var called bool
	o := defaultOptions()
	WithOnStart(func(ctx context.Context) { called = true })(&o)
	require.NotNil(t, o.onStart)

	o.onStart(context.Background())
	require.True(t, called)
}

// TestWithOnStop проверяет, что колбэк сохраняется и вызывается.
func TestWithOnStop(t *testing.T) {
	t.Parallel()

	var called bool
	o := defaultOptions()
	WithOnStop(func(ctx context.Context) { called = true })(&o)
	require.NotNil(t, o.onStop)

	o.onStop(context.Background())
	require.True(t, called)
}

// TestWithOnError проверяет, что колбэк сохраняется и получает ошибку.
func TestWithOnError(t *testing.T) {
	t.Parallel()

	var got error
	o := defaultOptions()
	WithOnError(func(ctx context.Context, err error) { got = err })(&o)
	require.NotNil(t, o.onError)

	expected := errors.New("boom")
	o.onError(context.Background(), expected)
	require.Equal(t, expected, got)
}

// TestWithOnPanic проверяет, что колбэк сохраняется и получает значение паники.
func TestWithOnPanic(t *testing.T) {
	t.Parallel()

	var got any
	o := defaultOptions()
	WithOnPanic(func(ctx context.Context, v any) { got = v })(&o)
	require.NotNil(t, o.onPanic)

	o.onPanic(context.Background(), "panic-value")
	require.Equal(t, "panic-value", got)
}

// TestOptions_AppliedInOrder проверяет, что при передаче нескольких опций
// каждая применяется к одному и тому же options.
func TestOptions_AppliedInOrder(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewTextHandler(nil, nil))
	var startCalled bool

	o := defaultOptions()
	for _, opt := range []Option{
		WithPromRegistry(reg),
		WithLogger(logger),
		WithOnStart(func(ctx context.Context) { startCalled = true }),
	} {
		opt(&o)
	}

	require.Equal(t, reg, o.registry)
	require.Same(t, logger, o.logger)
	require.NotNil(t, o.onStart)
	o.onStart(context.Background())
	require.True(t, startCalled)
}
