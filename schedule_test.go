package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNewProcessor проверяет создание процессора с параметрами по умолчанию.
func TestNewProcessor(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	p := NewPeriodTask(cfg, func(ctx context.Context) (bool, error) { return false, nil })
	require.NotNil(t, p)
	require.True(t, p.config.Name == DefaultName)
	require.True(t, p.config.Namespace == DefaultNamespace)
	require.True(t, p.config.Concurrency == DefaultConcurrency)
	require.True(t, p.config.MaxRetries == DefaultMaxRetry)
	require.True(t, p.config.Interval == DefaultInterval)
	require.True(t, p.config.RetryDelay == DefaultRetryDelay)
	require.NotNil(t, p.config.Registry)
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
		MaxRetries:  0,
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
		MaxRetries:  3,
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

	// Должно быть минимум MaxRetries попыток (3), но может быть больше, если успеет ещё одна задача.
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
		MaxRetries:  0,
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
		MaxRetries:  0,
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
		MaxRetries:  0,
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
		MaxRetries:  0,
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
		MaxRetries:  0,
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
		MaxRetries:  0,
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
	p.m.IncTasksTotal("test", true, false)
	p.m.ObserveTotalDuration("test", time.Second)
	p.m.ObserveTaskDuration("test", time.Second)
	p.m.ObserveWaitDuration("test", time.Second)
	p.m.SetSemaphoreUsed("test", 5)
}
