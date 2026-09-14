package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	DefaultInterval       = 5 * time.Second
	DefaultMetricInterval = 30 * time.Second
	DefaultMaxAttempts    = 1
	DefaultConcurrency    = 1
	DefaultTimeout        = 30 * time.Second
	DefaultName           = "DEFAULT"
	DefaultRetryDelay     = time.Second
)

func DefaultConfig() Config {
	return Config{
		Name:           DefaultName,
		Interval:       DefaultInterval,
		MetricInterval: DefaultMetricInterval,
		MaxAttempts:    DefaultMaxAttempts,
		Concurrency:    DefaultConcurrency,
		Timeout:        DefaultTimeout,
		RetryDelay:     DefaultRetryDelay,
	}
}

func NewPeriodTask(c Config, h func(ctx context.Context) (bool, error), opts ...Option) *Processor {
	config := DefaultConfig()
	if c.Interval <= 0 {
		c.Interval = config.Interval
	}
	if c.MetricInterval <= 0 {
		c.MetricInterval = config.MetricInterval
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = config.MaxAttempts
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = config.RetryDelay
	}
	if c.Concurrency <= 0 {
		c.Concurrency = config.Concurrency
	}
	if c.Timeout <= 0 {
		c.Timeout = config.Timeout
	}
	if len(c.Name) == 0 {
		c.Name = config.Name
	}
	options := defaultOptions()

	for _, opt := range opts {
		opt(&options)
	}

	return &Processor{
		config:  c,
		fn:      h,
		stopCh:  make(chan struct{}),
		sem:     make(chan struct{}, c.Concurrency),
		release: make(chan bool, c.Concurrency),
		metrics: newMetrics(options.registry),
		logger:  options.logger,
		hooks: hooks{
			onStart: options.onStart,
			onStop:  options.onStop,
			onError: options.onError,
			onPanic: options.onPanic,
		},
	}
}

type hooks struct {
	onStart func(ctx context.Context)
	onStop  func(ctx context.Context)
	onError func(ctx context.Context, err error)
	onPanic func(ctx context.Context, val any)
}

type Processor struct {
	mu      sync.Mutex
	logger  *slog.Logger
	wg      sync.WaitGroup
	config  Config
	hooks   hooks
	stopCh  chan struct{}
	sem     chan struct{} // семафор ограничения конкурентности
	release chan bool     // канал сигналов завершения (true = были сообщения)
	fn      func(ctx context.Context) (bool, error)
	metrics *metrics
	started bool
	stopped bool
}

type Config struct {
	Name           string
	MetricInterval time.Duration
	Interval       time.Duration
	Timeout        time.Duration
	RetryDelay     time.Duration
	MaxAttempts    int
	Concurrency    int
}

// Start - запуск воркера
func (p *Processor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return fmt.Errorf("processor %s: start after stop is not supported", p.config.Name)
	}

	if p.started {
		p.mu.Unlock()
		p.logger.Warn("processor already started", slog.String("name", p.config.Name))

		return nil
	}
	p.started = true
	p.wg.Add(1)
	p.mu.Unlock()

	var (
		schedule        = time.NewTimer(0)
		scheduleMetrics = time.NewTimer(p.config.MetricInterval)
	)

	p.logger.Info("processor started",
		slog.String("name", p.config.Name),
		slog.Duration("interval", p.config.Interval),
		slog.Int("concurrency", p.config.Concurrency),
		slog.Int("max_attempts", p.config.MaxAttempts),
		slog.Duration("retry_delay", p.config.RetryDelay),
	)
	p.metrics.SetSemaphoreUsed(p.config.Name, 0)

	go func() {
		defer func() {
			scheduleMetrics.Stop()
			schedule.Stop()
			p.wg.Done()
		}()

		p.onStart(ctx)
		for {
			select {
			case <-ctx.Done():
				p.logger.Info("processor stopping", slog.String("processor", p.config.Name))
				return
			case <-p.stopCh:
				p.logger.Info("processor stopping", slog.String("processor", p.config.Name))
				return
			case <-schedule.C:
				// плановый запуск
				schedule.Reset(p.config.Interval)
				p.tryStartTask(ctx)
			case <-scheduleMetrics.C:
				p.metrics.SetSemaphoreUsed(p.config.Name, len(p.sem))
			case wait := <-p.release:
				// горутина завершилась, завершенная гоуртина сообщает нужно ли ждать
				if !wait {
					p.tryStartTask(ctx)
					// Таймер при этом не трогаем он уже либо остановлен (если не был перезапущен),
					// либо будет перезапущен при необходимости
				} else {
					// Сообщаем что нужно попробовать следующую попытку через Interval
					schedule.Reset(p.config.Interval)
				}
			}
		}
	}()

	return nil
}

// tryStart пытается запустить новую горутину, если есть место в семафоре
func (p *Processor) tryStartTask(ctx context.Context) bool {
	waitStart := time.Now()

	select {
	case p.sem <- struct{}{}:
		// Время ожидания семафора
		waitDuration := time.Since(waitStart)

		p.wg.Add(1)
		go func(startedAt time.Time, waitDuration time.Duration) {
			var (
				wait    bool
				recov   bool
				attempt int
				err     error

				executeStart = time.Now()
			)
			defer func() { p.wg.Done() }()

			defer func() {
				if r := recover(); r != nil {
					recov = true
					wait = true
					p.logger.Error("processor task panic",
						slog.String("processor", p.config.Name),
						slog.Any("recover", r),
					)
					p.onPanic(ctx, r)
				}

				p.metrics.IncTasksTotal(p.config.Name, err == nil && !recov, attempt > 0)
				p.metrics.ObserveTotalDuration(p.config.Name, time.Since(startedAt))
				p.metrics.ObserveTaskDuration(p.config.Name, time.Since(executeStart))
				p.metrics.ObserveWaitDuration(p.config.Name, waitDuration)

				<-p.sem

				select {
				case p.release <- wait:
					return
				case <-p.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}()

			wait, attempt, err = p.executeWithRetry(ctx)

			// Логируем с детализацией
			logAttrs := []any{
				slog.String("processor", p.config.Name),
				slog.Duration("wait_duration", waitDuration), // время ожидания семафора
				slog.Duration("execute_duration", time.Since(executeStart)),
				slog.Duration("total_duration", time.Since(startedAt)),
				slog.Time("started_at", startedAt),
				slog.Int("concurrency", p.config.Concurrency),
				slog.Int("sem_used", len(p.sem)),
				slog.Int("sem_available", cap(p.sem)-len(p.sem)),
				slog.Int("attempt", attempt),
			}

			if err != nil {
				p.onError(ctx, err)
				logAttrs = append(logAttrs, slog.String("error", err.Error()))
				p.logger.Error("processor task failed", logAttrs...)
			}
			p.logger.Debug("processor task completed", logAttrs...)
		}(time.Now(), waitDuration)
	case <-ctx.Done():
		// процессор заверщен
		return false
	case <-p.stopCh:
		// процессор заверщен
		return false
	default:
		// семафор заполнен
		return false
	}

	return true
}

// executeWithRetry выполняет хендлер с повторными попытками при ошибке.
// Возвращает результат wait и последнюю ошибку (или nil при успехе).
func (p *Processor) executeWithRetry(ctx context.Context) (wait bool, attempt int, err error) {
	for attempt = range p.config.MaxAttempts {
		if attempt > 0 {
			backoff := time.Duration(attempt) * p.config.RetryDelay
			select {
			case <-ctx.Done():
				return true, attempt, ctx.Err()
			case <-time.After(backoff):
			}
		}
		ctxReq, cancel := context.WithTimeout(ctx, p.config.Timeout)
		wait, err = p.fn(ctxReq)
		cancel()
		if err == nil {
			return wait, attempt, nil
		}
	}
	// Все попытки исчерпаны, возвращаем ожидание и последнюю ошибку
	return true, attempt, err
}

// Stop - остановка воркера
func (p *Processor) Stop() {
	p.mu.Lock()
	if !p.started || p.stopped {
		p.mu.Unlock()
		return
	}
	p.started = false
	p.stopped = true
	p.mu.Unlock()

	p.logger.Info("stopped processor", slog.String("processor", p.config.Name))
	close(p.stopCh)
	p.wg.Wait()

	close(p.sem)
	close(p.release)

	p.onStop(context.Background())
}
