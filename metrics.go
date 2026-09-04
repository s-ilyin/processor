package processor

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	processorTasksTotal    *prometheus.CounterVec // total, success, error
	processorSemaphoreUsed *prometheus.GaugeVec
	processorTaskDuration  *prometheus.HistogramVec
	processorWaitDuration  *prometheus.HistogramVec
	processorTotalDuration *prometheus.HistogramVec
}

var (
	singal sync.Once
	gm     *metrics
)

func newMetrics(namespace string, reg prometheus.Registerer) *metrics {
	singal.Do(func() {
		m := &metrics{}
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}
		builder := promauto.With(reg)

		m.processorTasksTotal = builder.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "processor_tasks_total",
			Help:      "Total number of task executions",
		}, []string{"name", "is_succses", "is_retry"})

		m.processorSemaphoreUsed = builder.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processor_semaphore_used",
			Help:      "Current semaphore capacity slot (total, used, available)",
		}, []string{"name", "state"})

		m.processorTotalDuration = builder.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processor_total_duration_seconds",
			Help:      "Duration of total execution",
			Buckets:   prometheus.DefBuckets,
		}, []string{"name"})

		m.processorTaskDuration = builder.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processor_task_duration_seconds",
			Help:      "Duration of task execution (including retries)",
			Buckets:   prometheus.DefBuckets,
		}, []string{"name"})

		m.processorWaitDuration = builder.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processor_wait_duration_seconds",
			Help:      "Time spent waiting for semaphore before task start",
			Buckets:   prometheus.DefBuckets,
		}, []string{"name"})

		gm = m
	})

	return gm
}

// IncTasksTotal увеличивает счётчик выполненных задач.
// isRetry true, если задача была повторной попыткой; isSuccess – true при успешном выполнении.
func (m *metrics) IncTasksTotal(name string, isSuccess, isRetry bool) {
	m.processorTasksTotal.WithLabelValues(name, strconv.FormatBool(isSuccess), strconv.FormatBool(isRetry)).Inc()
}

// ObserveTaskDuration записывает время выполнения задачи (в секундах).
func (m *metrics) ObserveTotalDuration(name string, duration time.Duration) {
	m.processorTotalDuration.WithLabelValues(name).Observe(float64(duration.Seconds()))
}

// ObserveTaskDuration записывает время выполнения задачи (в секундах).
func (m *metrics) ObserveTaskDuration(name string, duration time.Duration) {
	m.processorTaskDuration.WithLabelValues(name).Observe(float64(duration.Seconds()))
}

// ObserveWaitDuration записывает время ожидания семафора (в секундах).
func (m *metrics) ObserveWaitDuration(name string, duration time.Duration) {
	m.processorWaitDuration.WithLabelValues(name).Observe(float64(duration.Seconds()))
}

// SetSemaphoreUsed – обновляет только количество используемых слотов.
func (m *metrics) SetSemaphoreUsed(name string, used int) {
	m.processorSemaphoreUsed.WithLabelValues(name, "used").Add(float64(used))
}
