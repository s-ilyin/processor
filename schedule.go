package processor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	DefaultInterval    = 5 * time.Second
	DefaultMaxRetry    = 1
	DefaultConcurrency = 1
	DefaultTimeout     = 30 * time.Second
	DefaultName        = "DEFAULT"
	DefaultNamespace   = "default"
	DefaultRetryDelay  = time.Second
)

func DefaultConfig() Config {
	return Config{
		Name:        DefaultName,
		Namespace:   DefaultNamespace,
		Interval:    DefaultInterval,
		MaxRetries:  DefaultMaxRetry,
		Concurrency: DefaultConcurrency,
		Timeout:     DefaultTimeout,
		RetryDelay:  DefaultRetryDelay,
		Registry:    prometheus.NewRegistry(),
	}
}

func NewPeriodTask(c Config, h func(ctx context.Context) (bool, error)) *Processor {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = DefaultMaxRetry
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = DefaultRetryDelay
	}
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if len(c.Name) == 0 {
		c.Name = DefaultName
	}
	if len(c.Namespace) == 0 {
		c.Namespace = DefaultNamespace
	}

	return &Processor{
		config:  c,
		fn:      h,
		stopCh:  make(chan struct{}),
		sem:     make(chan struct{}, c.Concurrency),
		release: make(chan bool, c.Concurrency),
		m:       newMetrics(c.Namespace, c.Registry),
	}
}

type Processor struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	config  Config
	stopCh  chan struct{}
	sem     chan struct{} // семафор ограничения конкурентности
	release chan bool     // канал сигналов завершения (true = были сообщения)
	fn      func(ctx context.Context) (bool, error)
	m       *metrics
	started bool
}

type Config struct {
	Name        string
	Namespace   string
	Interval    time.Duration
	Timeout     time.Duration
	RetryDelay  time.Duration
	MaxRetries  int
	Concurrency int
	Registry    prometheus.Registerer
}

// Start - запуск воркера
func (p *Processor) Start(ctx context.Context) {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		slog.Warn("processor already started", slog.String("name", p.config.Name))
		return
	}
	p.started = true
	p.mu.Unlock()

	schedule := time.NewTimer(0)

	slog.Info("processor started",
		slog.String("name", p.config.Name),
		slog.Duration("interval", p.config.Interval),
		slog.Int("concurrency", p.config.Concurrency),
		slog.Int("max_retries", p.config.MaxRetries),
		slog.Duration("retry_delay", p.config.RetryDelay),
	)
	p.m.SetSemaphoreUsed(p.config.Name, 0)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer schedule.Stop()
		for {
			select {
			case <-ctx.Done():
				slog.Info("processor stopping", slog.String("processor", p.config.Name))
				return
			case <-p.stopCh:
				slog.Info("processor stopping", slog.String("processor", p.config.Name))
				return
			case <-schedule.C:
				// плановый запуск
				schedule.Reset(p.config.Interval)
				p.tryStartTask(ctx)
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
}

// tryStart пытается запустить новую горутину, если есть место в семафоре
func (p *Processor) tryStartTask(ctx context.Context) bool {
	waitStart := time.Now()

	select {
	case p.sem <- struct{}{}:
		// Время ожидания семафора
		waitDuration := time.Since(waitStart)
		p.m.SetSemaphoreUsed(p.config.Name, len(p.sem))

		p.wg.Add(1)
		go func(startedAt time.Time, waitDuration time.Duration) {
			var wait bool

			defer func() {
				<-p.sem
				p.m.SetSemaphoreUsed(p.config.Name, len(p.sem))
				p.wg.Done()
			}()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("processor task panic",
						slog.String("processor", p.config.Name),
						slog.Any("recover", r),
					)
				}

				select {
				case p.release <- wait:
					return
				default:
					return
				}
			}()

			ctxReq, cancel := context.WithTimeout(ctx, p.config.Timeout)
			defer cancel()

			var (
				attempt int
				err     error

				executeStart = time.Now()
			)
			wait, attempt, err = p.executeWithRetry(ctxReq)

			var (
				executeDuration = time.Since(executeStart)
				totalDuration   = time.Since(startedAt)

				// Логируем с детализацией
				logAttrs = []any{
					slog.String("namespace", p.config.Namespace),
					slog.String("processor", p.config.Name),
					slog.Duration("wait_duration", waitDuration), // время ожидания семафора
					slog.Duration("execute_duration", executeDuration),
					slog.Duration("total_duration", totalDuration),
					slog.Time("started_at", startedAt),
					slog.Int("concurrency", p.config.Concurrency),
					slog.Int("sem_used", len(p.sem)),
					slog.Int("sem_available", cap(p.sem)-len(p.sem)),
					slog.Int("attempt", attempt),
				}
			)

			p.m.IncTasksTotal(p.config.Name, err == nil, attempt > 0)
			p.m.ObserveTotalDuration(p.config.Name, totalDuration)
			p.m.ObserveTaskDuration(p.config.Name, executeDuration)
			p.m.ObserveWaitDuration(p.config.Name, waitDuration)
			p.m.SetSemaphoreUsed(p.config.Name, len(p.sem))

			if err != nil {
				logAttrs = append(logAttrs, slog.String("error", err.Error()))
				slog.Error("processor task failed", logAttrs...)
			}
			slog.Debug("processor task completed", logAttrs...)
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
	for attempt = range p.config.MaxRetries {
		if attempt > 0 {
			backoff := time.Duration(attempt) * p.config.RetryDelay
			select {
			case <-ctx.Done():
				return true, attempt, ctx.Err()
			case <-time.After(backoff):
			}
		}

		wait, err = p.fn(ctx)
		if err == nil {
			return wait, attempt, nil
		}
	}
	// Все попытки исчерпаны, возвращаем ожидание и последнюю ошибку
	return true, attempt, err
}

// Stop - остановка воркера
func (p *Processor) Stop() {
	slog.Info("stop processor", slog.String("processor", p.config.Name))
	close(p.stopCh)
	p.wg.Wait()
	close(p.sem)
	close(p.release)
	p.mu.Lock()
	p.started = false
	p.mu.Unlock()
}
