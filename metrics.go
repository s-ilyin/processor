package processor

import (
	"errors"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	processorTasksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "processor_tasks_total",
		Help: "Total number of task executions",
	}, []string{"name", "is_success", "is_retry"})

	processorSemaphoreUsed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "processor_semaphore_used",
		Help: "Current number of used semaphore slots",
	}, []string{"name", "state"})

	processorTotalDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "processor_total_duration_seconds",
		Help:    "Duration of total execution",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})

	processorTaskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "processor_task_duration_seconds",
		Help:    "Duration of task execution (including retries)",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})

	processorWaitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "processor_wait_duration_seconds",
		Help:    "Time spent waiting for semaphore before task start",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})
)

type metrics struct {
	processorTasksTotal    *prometheus.CounterVec
	processorSemaphoreUsed *prometheus.GaugeVec
	processorTaskDuration  *prometheus.HistogramVec
	processorWaitDuration  *prometheus.HistogramVec
	processorTotalDuration *prometheus.HistogramVec
}

// newMetrics регистрирует метрики в переданном реестре и возвращает обёртку.
// Если reg == nil — используется prometheus.DefaultRegisterer.
// Повторная регистрация того же самого коллектора в том же реестре безопасна
// (несколько Processor-ов могут делить один реестр). Если имя метрики занято
// чужим коллектором — паникует.
func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	register(reg,
		processorTasksTotal,
		processorSemaphoreUsed,
		processorTotalDuration,
		processorTaskDuration,
		processorWaitDuration,
	)

	return &metrics{
		processorTasksTotal:    processorTasksTotal,
		processorSemaphoreUsed: processorSemaphoreUsed,
		processorTotalDuration: processorTotalDuration,
		processorTaskDuration:  processorTaskDuration,
		processorWaitDuration:  processorWaitDuration,
	}
}

// register регистрирует коллекторы в реестре.
// Если тот же самый коллектор уже зарегистрирован — пропускает.
// Если имя занято чужим коллектором — паникует.
func register(reg prometheus.Registerer, collectors ...prometheus.Collector) {
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) && already.ExistingCollector == c {
				continue
			}
			panic(err)
		}
	}
}

func (m *metrics) IncTasksTotal(name string, isSuccess, isRetry bool) {
	m.processorTasksTotal.WithLabelValues(
		name,
		strconv.FormatBool(isSuccess),
		strconv.FormatBool(isRetry),
	).Inc()
}

func (m *metrics) ObserveTotalDuration(name string, duration time.Duration) {
	m.processorTotalDuration.WithLabelValues(name).Observe(duration.Seconds())
}

func (m *metrics) ObserveTaskDuration(name string, duration time.Duration) {
	m.processorTaskDuration.WithLabelValues(name).Observe(duration.Seconds())
}

func (m *metrics) ObserveWaitDuration(name string, duration time.Duration) {
	m.processorWaitDuration.WithLabelValues(name).Observe(duration.Seconds())
}

func (m *metrics) SetSemaphoreUsed(name string, used int) {
	m.processorSemaphoreUsed.WithLabelValues(name, "used").Set(float64(used))
}
