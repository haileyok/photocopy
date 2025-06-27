package clickhouse_inserter

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Inserter struct {
	conn           driver.Conn
	query          string
	mu             sync.Mutex
	queuedEvents   []any
	batchSize      int
	insertsCounter *prometheus.CounterVec
	pendingSends   prometheus.Gauge
	histogram      *prometheus.HistogramVec
	logger         *slog.Logger
	prefix         string
}

type Args struct {
	Conn                    driver.Conn
	Query                   string
	BatchSize               int
	PrometheusCounterPrefix string
	Logger                  *slog.Logger
	Histogram               *prometheus.HistogramVec
}

func New(ctx context.Context, args *Args) (*Inserter, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	inserter := &Inserter{
		conn:      args.Conn,
		query:     args.Query,
		mu:        sync.Mutex{},
		batchSize: args.BatchSize,
		histogram: args.Histogram,
		logger:    args.Logger,
		prefix:    args.PrometheusCounterPrefix,
	}

	if args.PrometheusCounterPrefix != "" {
		inserter.insertsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
			Name:      "clickhouse_inserts",
			Namespace: args.PrometheusCounterPrefix,
			Help:      "total inserts into clickhouse by status",
		}, []string{"status"})

		inserter.pendingSends = promauto.NewGauge(prometheus.GaugeOpts{
			Name:      "clickhouse_pending_sends",
			Namespace: args.PrometheusCounterPrefix,
			Help:      "total clickhouse insertions that are in progress",
		})

	} else {
		args.Logger.Info("no prometheus prefix provided, no metrics will be registered for this counter", "query", args.Query)
	}

	return inserter, nil
}

func (i *Inserter) Insert(ctx context.Context, e any) error {
	i.mu.Lock()

	i.queuedEvents = append(i.queuedEvents, e)

	var toInsert []any
	if len(i.queuedEvents) >= i.batchSize {
		toInsert = slices.Clone(i.queuedEvents)
		i.queuedEvents = nil
	}

	i.mu.Unlock()

	if len(toInsert) > 0 {
		i.sendStream(ctx, toInsert)
	}

	return nil
}

func (i *Inserter) Close(ctx context.Context) error {
	i.mu.Lock()

	var toInsert []any

	if len(i.queuedEvents) > 0 {
		toInsert = slices.Clone(i.queuedEvents)
		i.queuedEvents = nil
	}

	i.mu.Unlock()

	if len(toInsert) > 0 {
		i.sendStream(ctx, toInsert)
	}

	return nil
}

func (i *Inserter) sendStream(ctx context.Context, toInsert []any) {
	i.pendingSends.Inc()
	defer i.pendingSends.Dec()

	if i.histogram != nil {
		start := time.Now()
		defer func() {
			i.histogram.WithLabelValues(i.prefix).Observe(time.Since(start).Seconds())
		}()
	}

	if len(toInsert) == 0 {
		return
	}

	status := "ok"
	defer func() {
		i.insertsCounter.WithLabelValues(status).Add(float64(len(toInsert)))
	}()

	batch, err := i.conn.PrepareBatch(ctx, i.query)
	if err != nil {
		i.logger.Error("error creating batch", "prefix", i.prefix, "error", err)
		status = "failed"
		return
	}

	for _, d := range toInsert {
		if err := batch.AppendStruct(&d); err != nil {
			i.logger.Error("error appending to batch", "prefix", i.prefix, "error", err)
		}
	}

	if err := batch.Send(); err != nil {
		status = "failed"
		i.logger.Error("error sending batch", "prefix", i.prefix, "error", err)
	}
}
