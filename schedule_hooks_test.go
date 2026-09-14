package processor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// newProcessorWithHooks создаёт Processor с заданными хуками без запуска.
func newProcessorWithHooks(t *testing.T, h hooks) *Processor {
	t.Helper()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	})
	p.hooks = h
	p.logger = logger

	return p
}

// TestOnStart_InvokesHook проверяет, что onStart делегирует в хук.
func TestOnStart_InvokesHook(t *testing.T) {
	t.Parallel()

	var called bool
	p := newProcessorWithHooks(t, hooks{
		onStart: func(ctx context.Context) { called = true },
	})

	p.onStart(context.Background())
	require.True(t, called)
}

// TestOnStart_NilHookNoop проверяет, что при nil-хуке ничего не происходит.
func TestOnStart_NilHookNoop(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{})
	require.NotPanics(t, func() { p.onStart(context.Background()) })
}

// TestOnStop_InvokesHook проверяет, что onStop делегирует в хук.
func TestOnStop_InvokesHook(t *testing.T) {
	t.Parallel()

	var called bool
	p := newProcessorWithHooks(t, hooks{
		onStop: func(ctx context.Context) { called = true },
	})

	p.onStop(context.Background())
	require.True(t, called)
}

// TestOnStop_NilHookNoop проверяет, что при nil-хуке ничего не происходит.
func TestOnStop_NilHookNoop(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{})
	require.NotPanics(t, func() { p.onStop(context.Background()) })
}

// TestOnError_InvokesHook проверяет, что onError делегирует в хук и
// прокидывает исходную ошибку без обёртки.
func TestOnError_InvokesHook(t *testing.T) {
	t.Parallel()

	var got error
	p := newProcessorWithHooks(t, hooks{
		onError: func(ctx context.Context, err error) { got = err },
	})

	expected := errors.New("test error")
	p.onError(context.Background(), expected)
	require.Equal(t, expected, got)
}

// TestOnError_NilHookNoop проверяет, что при nil-хуке ничего не происходит.
func TestOnError_NilHookNoop(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{})
	require.NotPanics(t, func() { p.onError(context.Background(), errors.New("x")) })
}

// TestOnPanic_InvokesHook проверяет, что onPanic делегирует в хук.
// Отдельно проверяем, что старый баг с проверкой onError вместо onPanic
// не вернулся.
func TestOnPanic_InvokesHook(t *testing.T) {
	t.Parallel()

	var got any
	p := newProcessorWithHooks(t, hooks{
		onPanic: func(ctx context.Context, v any) { got = v },
	})

	p.onPanic(context.Background(), "recovered")
	require.Equal(t, "recovered", got)
}

// TestOnPanic_NilHookNoop проверяет, что при nil-хуке onPanic
// ничего не делает и не паникует (не вызывает nil-fn).
func TestOnPanic_NilHookNoop(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{})
	require.NotPanics(t, func() { p.onPanic(context.Background(), "value") })
}

// TestSafeHook_RecoversFromPanic проверяет, что паника внутри хука
// перехватывается safeHook и не валит планировщик.
func TestSafeHook_RecoversFromPanic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := newProcessorWithHooks(t, hooks{})
	p.logger = slog.New(slog.NewTextHandler(&buf, nil))

	require.NotPanics(t, func() {
		p.safeHook("test_hook", func() {
			panic("boom inside hook")
		})
	})

	out := buf.String()
	require.Contains(t, out, "hook panic")
	require.Contains(t, out, "test_hook")
	require.Contains(t, out, "boom inside hook")
}

// TestSafeHook_NilFn проверяет, что nil-fn не приводит к панике.
func TestSafeHook_NilFn(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{})
	require.NotPanics(t, func() { p.safeHook("test", nil) })
}

// TestOnError_HookPanicDoesNotPropagate проверяет, что паника внутри
// пользовательского onError не пробрасывается наружу.
func TestOnError_HookPanicDoesNotPropagate(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{
		onError: func(ctx context.Context, err error) {
			panic("hook wants to break scheduler")
		},
	})

	require.NotPanics(t, func() {
		p.onError(context.Background(), errors.New("task failed"))
	})
}

// TestOnPanic_HookPanicDoesNotPropagate — то же для onPanic.
func TestOnPanic_HookPanicDoesNotPropagate(t *testing.T) {
	t.Parallel()

	p := newProcessorWithHooks(t, hooks{
		onPanic: func(ctx context.Context, v any) {
			panic("hook panics on panic")
		},
	})

	require.NotPanics(t, func() {
		p.onPanic(context.Background(), "recovered value")
	})
}
