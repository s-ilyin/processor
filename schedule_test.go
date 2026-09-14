package processor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestNewProcessor проверяет создание процессора с параметрами по умолчанию.
func TestNewProcessor(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	p := NewPeriodTask(cfg, func(ctx context.Context) (bool, error) { return false, nil })
	require.NotNil(t, p)
	require.True(t, p.config.Name == DefaultName)
	require.True(t, p.config.Concurrency == DefaultConcurrency)
	require.True(t, p.config.MaxAttempts == DefaultMaxAttempts)
	require.True(t, p.config.Interval == DefaultInterval)
	require.True(t, p.config.RetryDelay == DefaultRetryDelay)
}

// TestProcessorStartStop проверяет базовый запуск и остановку без выполнения задач.
func TestProcessorStartStop(t *testing.T) {
	t.Parallel()

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return false, nil
	})
	ctx := t.Context()

	p.Start(ctx)
	time.Sleep(50 * time.Millisecond) // даём время на инициализацию
	p.Stop()
}

// TestProcessorHandleSuccess проверяет, что хендлер вызывается и возвращает успех.
func TestProcessorHandleSuccess(t *testing.T) {
	t.Parallel()

	var calls int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    50 * time.Millisecond,
		MaxAttempts: 0,
		Concurrency: 1,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return false, nil // wait=false, чтобы запускались часто
	})

	ctx := t.Context()

	p.Start(ctx)
	time.Sleep(200 * time.Millisecond) // даём время на несколько итераций
	p.Stop()

	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("expected at least 2 calls, got %d", calls)
	}
}

// TestProcessorHandlerErrorRetries проверяет, что при ошибке выполняются повторные попытки.
func TestProcessorHandleErrorRetries(t *testing.T) {
	t.Parallel()

	var attempts int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    100 * time.Millisecond,
		MaxAttempts: 3,
		Concurrency: 1,
		Timeout:     1 * time.Second,
		RetryDelay:  100 * time.Millisecond,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&attempts, 1)
		return false, errors.New("always fails")
	})

	ctx := t.Context()

	p.Start(ctx)
	time.Sleep(500 * time.Millisecond) // даём время на выполнение попыток
	p.Stop()

	// Должно быть минимум MaxAttempts попыток (3), но может быть больше, если успеет ещё одна задача.
	// Убедимся, что хотя бы 3 попытки были.
	if atomic.LoadInt32(&attempts) < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts)
	}
}

// TestProcessorPanicRecovery проверяет, что паника в хендлере перехватывается и не останавливает планировщик.
func TestProcessorPanicRecovery(t *testing.T) {
	t.Parallel()

	var calls int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    100 * time.Millisecond,
		MaxAttempts: 0,
		Concurrency: 1,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		panic("test panic")
	})

	ctx := t.Context()

	p.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	p.Stop()

	// После паники планировщик должен продолжать работать, поэтому вызовов должно быть > 1.
	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("expected at least 2 calls after panic recovery, got %d", calls)
	}
}

// TestSemaphoreConcurrency проверяет, что одновременно работает не более Concurrency задач.
func TestSemaphoreConcurrency(t *testing.T) {
	t.Parallel()

	const concurrency = 3
	var (
		concurrent    int32
		maxConcurrent int32
	)

	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    10 * time.Millisecond,
		MaxAttempts: 0,
		Concurrency: concurrency,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		cur := atomic.AddInt32(&concurrent, 1)
		defer atomic.AddInt32(&concurrent, -1)

		// Обновляем максимум
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if cur <= old {
				break
			}
			if atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		return false, nil
	})

	ctx := t.Context()

	p.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	p.Stop()

	require.LessOrEqual(t, atomic.LoadInt32(&maxConcurrent), int32(concurrency), "max concurrency exceeded: %d > %d", maxConcurrent, concurrency)
	require.Greater(t, atomic.LoadInt32(&maxConcurrent), int32(1), "expected concurrency > 1, got %d", maxConcurrent)
}

// TestImmediateRestartOnWaitFalse проверяет, что при wait=false следующая задача запускается немедленно.
func TestImmediateRestartOnWaitFalse(t *testing.T) {
	t.Parallel()

	var callCount int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    1 * time.Second,
		MaxAttempts: 0,
		Concurrency: 1,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&callCount, 1)
		return false, nil
	})

	ctx := t.Context()
	p.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	p.Stop()

	require.GreaterOrEqual(t, atomic.LoadInt32(&callCount), int32(2),
		"expected at least 2 calls (immediate restart), got %d", atomic.LoadInt32(&callCount))
}

func TestNoImmediateRestartOnWaitTrue(t *testing.T) {
	t.Parallel()

	var callCount int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    200 * time.Millisecond,
		MaxAttempts: 0,
		Concurrency: 1,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&callCount, 1)
		return true, nil
	})

	ctx := t.Context()
	p.Start(ctx)
	time.Sleep(250 * time.Millisecond)
	p.Stop()

	calls := atomic.LoadInt32(&callCount)
	require.True(t, calls >= 1 && calls <= 3,
		"expected 1-3 calls, got %d", calls)
}

func TestTimerInitialImmediate(t *testing.T) {
	t.Parallel()

	var calls int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    10 * time.Second,
		MaxAttempts: 0,
		Concurrency: 1,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return true, nil
	})

	ctx := t.Context()
	start := time.Now()
	p.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	p.Stop()

	elapsed := time.Since(start)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1),
		"expected at least 1 call immediately")
	require.Less(t, elapsed, 500*time.Millisecond,
		"first call took too long: %v", elapsed)
}

func TestStopWhileRunning(t *testing.T) {
	t.Parallel()

	var running int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    10 * time.Millisecond,
		MaxAttempts: 0,
		Concurrency: 2,
		Timeout:     1 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&running, 1)
		defer atomic.AddInt32(&running, -1)
		time.Sleep(200 * time.Millisecond)
		return false, nil
	})

	ctx := t.Context()
	p.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	p.Stop()

	require.Equal(t, int32(0), atomic.LoadInt32(&running),
		"expected running tasks to be 0 after stop, got %d", atomic.LoadInt32(&running))
}

// TestContextCancellation проверяет остановку по отмене контекста.
func TestContextCancellation(t *testing.T) {
	t.Parallel()

	var calls int32
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return false, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	p.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel() // отменяем контекст
	p.Stop() // Stop тоже должен корректно завершиться

	// После отмены контекста новые задачи не запускаются, но уже запущенные могут завершиться.
	// Проверим, что воркер остановился без паники.
}

// TestConcurrentStarts проверяет защиту от повторного вызова Start.
func TestConcurrentStarts(t *testing.T) {
	t.Parallel()

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return false, nil
	})
	ctx := t.Context()

	p.Start(ctx)
	p.Start(ctx) // второй раз должен выдать warn и ничего не сделать
	// Проверим, что не паникует и не создаёт дублирующих горутин.
	// Можно проверить что Stop завершается нормально.
	p.Stop()
}

// TestMetricsRegistration проверяет, что метрики не паникуют при регистрации (просто вызов).
// В реальных тестах можно игнорировать, но для покрытия добавим.
func TestMetricsRegistration(t *testing.T) {
	t.Parallel()

	// Создаём процессор, метрики зарегистрируются автоматически.
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return false, nil
	})
	p.metrics.IncTasksTotal("test", true, false)
	p.metrics.ObserveTotalDuration("test", time.Second)
	p.metrics.ObserveTaskDuration("test", time.Second)
	p.metrics.ObserveWaitDuration("test", time.Second)
	p.metrics.SetSemaphoreUsed("test", 5)
}

// --- Хуки на уровне Processor ---

// TestProcessor_OnStartCalled проверяет, что OnStart вызывается после Start.
func TestProcessor_OnStartCalled(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	}, WithOnStart(func(ctx context.Context) { called.Store(true) }))

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, called.Load, time.Second, 5*time.Millisecond,
		"OnStart was not called")
	p.Stop()
}

// TestProcessor_OnStopCalled проверяет, что OnStop вызывается после Stop,
// причём строго после завершения всех задач.
func TestProcessor_OnStopCalled(t *testing.T) {
	t.Parallel()

	var (
		taskFinished atomic.Bool
		stopCalled   atomic.Bool
		orderOK      atomic.Bool
	)

	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    10 * time.Millisecond,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		time.Sleep(50 * time.Millisecond)
		taskFinished.Store(true)
		return true, nil
	}, WithOnStop(func(ctx context.Context) {
		stopCalled.Store(true)
		if taskFinished.Load() {
			orderOK.Store(true)
		}
	}))

	require.NoError(t, p.Start(t.Context()))
	time.Sleep(30 * time.Millisecond)
	p.Stop()

	require.True(t, stopCalled.Load(), "OnStop was not called")
	require.True(t, orderOK.Load(), "OnStop fired before task finished")
}

// TestProcessor_OnErrorCalled проверяет, что OnError получает исходную
// ошибку хендлера после исчерпания всех попыток.
func TestProcessor_OnErrorCalled(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("boom")
	var (
		mu     sync.Mutex
		called bool
		gotErr error
	)

	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		return true, expectedErr
	}, WithOnError(func(ctx context.Context, err error) {
		mu.Lock()
		defer mu.Unlock()
		called = true
		gotErr = err
	}))

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return called
	}, time.Second, 5*time.Millisecond, "OnError was not called")
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, expectedErr, gotErr)
}

// TestProcessor_OnPanicCalled проверяет, что OnPanic получает значение паники.
func TestProcessor_OnPanicCalled(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		called bool
		gotVal any
	)

	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		panic("test panic")
	}, WithOnPanic(func(ctx context.Context, v any) {
		mu.Lock()
		defer mu.Unlock()
		called = true
		gotVal = v
	}))

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return called
	}, time.Second, 5*time.Millisecond, "OnPanic was not called")
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "test panic", gotVal)
}

// TestProcessor_OnErrorNotCalledOnSuccess проверяет, что OnError не дёргается
// при успешном выполнении.
func TestProcessor_OnErrorNotCalledOnSuccess(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		return true, nil
	}, WithOnError(func(ctx context.Context, err error) {
		called.Store(true)
	}))

	require.NoError(t, p.Start(t.Context()))
	time.Sleep(100 * time.Millisecond)
	p.Stop()

	require.False(t, called.Load(), "OnError must not fire on success")
}

// TestProcessor_OnPanicNotCalledOnError проверяет, что OnPanic не дёргается
// при обычной ошибке хендлера.
func TestProcessor_OnPanicNotCalledOnError(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		return true, errors.New("regular error")
	}, WithOnPanic(func(ctx context.Context, v any) {
		called.Store(true)
	}))

	require.NoError(t, p.Start(t.Context()))
	time.Sleep(100 * time.Millisecond)
	p.Stop()

	require.False(t, called.Load(), "OnPanic must not fire on regular error")
}

// TestProcessor_HookPanicDoesNotStopScheduler проверяет, что паника
// внутри пользовательского хука не останавливает event loop.
func TestProcessor_HookPanicDoesNotStopScheduler(t *testing.T) {
	t.Parallel()

	var (
		taskCalls atomic.Int32
		hookCalls atomic.Int32
	)
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    20 * time.Millisecond,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		taskCalls.Add(1)
		return true, nil
	}, WithOnStart(func(ctx context.Context) {
		hookCalls.Add(1)
		panic("hook panic")
	}))

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, func() bool {
		return taskCalls.Load() >= 2
	}, time.Second, 5*time.Millisecond,
		"processor should keep running after hook panic")
	p.Stop()

	require.Equal(t, int32(1), hookCalls.Load(), "OnStart must be called once")
}

// TestProcessor_OnStartBeforeFirstTask проверяет, что OnStart успевает
// завершиться до старта первой задачи (нет гонки, которая была раньше).
func TestProcessor_OnStartBeforeFirstTask(t *testing.T) {
	t.Parallel()

	var (
		onStartDone atomic.Bool
		taskSawDone atomic.Bool
	)
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		if onStartDone.Load() {
			taskSawDone.Store(true)
		}
		return true, nil
	}, WithOnStart(func(ctx context.Context) {
		onStartDone.Store(true)
	}))

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, taskSawDone.Load, time.Second, 5*time.Millisecond,
		"first task started before OnStart finished")
	p.Stop()
}

// --- Start / Stop edge-cases ---

// TestProcessor_StartAfterStopReturnsError проверяет, что рестарт запрещён.
func TestProcessor_StartAfterStopReturnsError(t *testing.T) {
	t.Parallel()

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	})
	require.NoError(t, p.Start(t.Context()))
	p.Stop()

	err := p.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "start after stop")
}

// TestProcessor_StopIdempotent проверяет, что двойной Stop не паникует.
func TestProcessor_StopIdempotent(t *testing.T) {
	t.Parallel()

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	})
	require.NoError(t, p.Start(t.Context()))
	p.Stop()

	require.NotPanics(t, func() { p.Stop() })
	require.NotPanics(t, func() { p.Stop() })
}

// TestProcessor_StopWithoutStart проверяет, что Stop без Start — no-op.
func TestProcessor_StopWithoutStart(t *testing.T) {
	t.Parallel()

	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	})
	require.NotPanics(t, func() { p.Stop() })
}

// TestProcessor_StartTwiceSecondIsNoop проверяет, что второй Start
// возвращает nil и не создаёт второй event loop.
func TestProcessor_StartTwiceSecondIsNoop(t *testing.T) {
	t.Parallel()

	var startHookCalls atomic.Int32
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	}, WithOnStart(func(ctx context.Context) {
		startHookCalls.Add(1)
	}))

	require.NoError(t, p.Start(t.Context()))
	require.NoError(t, p.Start(t.Context()), "second Start must be no-op without error")
	require.NoError(t, p.Start(t.Context()))

	time.Sleep(100 * time.Millisecond)
	p.Stop()

	require.Equal(t, int32(1), startHookCalls.Load(),
		"OnStart must be called exactly once across multiple Start calls")
}

// TestProcessor_ContextCancellationStopsScheduler проверяет, что
// отмена контекста останавливает event loop без Stop.
func TestProcessor_ContextCancellationStopsScheduler(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    10 * time.Millisecond,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		calls.Add(1)
		return false, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, p.Start(ctx))

	// Даём накрутиться.
	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, time.Second, 5*time.Millisecond)

	cancel()

	// После отмены число вызовов должно стабилизироваться.
	time.Sleep(50 * time.Millisecond)
	snapshot := calls.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, snapshot, calls.Load(),
		"scheduler must stop producing tasks after ctx cancel")

	p.Stop()
}

// --- Retry-механика ---

// TestProcessor_MaxAttemptsExact проверяет, что MaxAttempts=3 даёт
// ровно 3 попытки, не 4.
func TestProcessor_MaxAttemptsExact(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 3,
		Concurrency: 1,
		Timeout:     time.Second,
		RetryDelay:  10 * time.Millisecond,
	}, func(ctx context.Context) (bool, error) {
		attempts.Add(1)
		return true, errors.New("always fails")
	})

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, func() bool {
		return attempts.Load() >= 3
	}, time.Second, 5*time.Millisecond)

	// Даём гипотетической 4-й попытке шанс выполниться.
	time.Sleep(150 * time.Millisecond)
	p.Stop()

	require.Equal(t, int32(3), attempts.Load(),
		"MaxAttempts=3 must produce exactly 3 attempts")
}

// TestProcessor_RetryBackoff проверяет, что между попытками соблюдается
// backoff = attempt * RetryDelay.
func TestProcessor_RetryBackoff(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		times []time.Time
	)
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 3,
		Concurrency: 1,
		Timeout:     time.Second,
		RetryDelay:  30 * time.Millisecond,
	}, func(ctx context.Context) (bool, error) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return true, errors.New("always fails")
	})

	require.NoError(t, p.Start(t.Context()))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(times) >= 3
	}, time.Second, 5*time.Millisecond)
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, times, 3)

	// attempt=1 → 30ms, attempt=2 → 60ms. С запасом на джиттер.
	delta1 := times[1].Sub(times[0])
	delta2 := times[2].Sub(times[1])

	require.GreaterOrEqual(t, delta1, 25*time.Millisecond,
		"first retry too fast: %v", delta1)
	require.GreaterOrEqual(t, delta2, 55*time.Millisecond,
		"second retry too fast: %v", delta2)
}

// TestProcessor_TimeoutCancelsHandler проверяет, что Timeout реально
// отменяет ctx, переданный хендлеру.
func TestProcessor_TimeoutCancelsHandler(t *testing.T) {
	t.Parallel()

	var (
		gotErr atomic.Value
		done   = make(chan struct{})
		once   sync.Once
	)
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     50 * time.Millisecond,
	}, func(ctx context.Context) (bool, error) {
		<-ctx.Done()
		gotErr.Store(ctx.Err())
		once.Do(func() { close(done) })
		return true, ctx.Err()
	})

	require.NoError(t, p.Start(t.Context()))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled by timeout")
	}
	p.Stop()

	require.ErrorIs(t, gotErr.Load().(error), context.DeadlineExceeded)
}

// --- Проброс контекста ---

// TestProcessor_ContextPropagationToHandler проверяет, что значения из ctx,
// переданного в Start, доходят до хендлера.
func TestProcessor_ContextPropagationToHandler(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	key := ctxKey{}

	var (
		got  atomic.Value
		done = make(chan struct{})
		once sync.Once
	)
	p := NewPeriodTask(Config{
		Name:        "test",
		Interval:    time.Hour,
		MaxAttempts: 1,
		Concurrency: 1,
		Timeout:     time.Second,
	}, func(ctx context.Context) (bool, error) {
		got.Store(ctx.Value(key))
		once.Do(func() { close(done) })
		return true, nil
	})

	ctx := context.WithValue(t.Context(), key, "propagated-value")
	require.NoError(t, p.Start(ctx))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called")
	}
	p.Stop()

	require.Equal(t, "propagated-value", got.Load())
}

// --- Интеграция опций ---

// TestProcessor_WithPromRegistry проверяет, что метрики реально попадают
// в кастомный реестр, переданный через опцию.
func TestProcessor_WithPromRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	}, WithPromRegistry(reg))

	// Записываем метрику вручную, чтобы Gather её увидел.
	p.metrics.IncTasksTotal("schedule_behavior_test", true, false)

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, f := range families {
		if f.GetName() == "processor_tasks_total" {
			found = true
			break
		}
	}
	require.True(t, found, "processor_tasks_total not found in custom registry")
}

// TestProcessor_WithPromRegistryShared проверяет, что два процессора
// могут делить один реестр без паники.
func TestProcessor_WithPromRegistryShared(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	handler := func(ctx context.Context) (bool, error) { return true, nil }

	require.NotPanics(t, func() {
		p1 := NewPeriodTask(DefaultConfig(), handler, WithPromRegistry(reg))
		p2 := NewPeriodTask(DefaultConfig(), handler, WithPromRegistry(reg))
		require.NotNil(t, p1)
		require.NotNil(t, p2)
	})
}

// TestProcessor_WithLogger проверяет, что кастомный логгер сохраняется.
func TestProcessor_WithLogger(t *testing.T) {
	t.Parallel()

	logger := slog.Default()
	p := NewPeriodTask(DefaultConfig(), func(ctx context.Context) (bool, error) {
		return true, nil
	}, WithLogger(logger))

	require.Same(t, logger, p.logger)
}
