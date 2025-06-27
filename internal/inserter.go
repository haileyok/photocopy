package inserter

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Inserter struct {
	db             driver.Conn
	mu             sync.Mutex
	queuedEvents   []any
	batchSize      int
	insertsCounter *prometheus.CounterVec
	pendingSends   prometheus.Gauge
	histogram      *prometheus.HistogramVec
	logger         *slog.Logger
	prefix         string
}

type BaseArgs struct {
	CredentialsPath string
	ProjectID       string
	DatasetID       string
	TableID         string
}

type Args struct {
	BaseArgs
	BatchSize               int
	PrometheusCounterPrefix string
	Logger                  *slog.Logger
	Histogram               *prometheus.HistogramVec
}

func New(ctx context.Context, args *Args) (*Inserter, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	// dataset := bqc.Dataset(args.DatasetID)
	// table := dataset.Table(args.TableID)

	inserter := &Inserter{}

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
		args.Logger.Info("no prometheus prefix provided, no metrics will be registered for this counter", "dataset", args.DatasetID, "table", args.TableID)
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

	// TODO: do the insert

	i.insertsCounter.WithLabelValues(status).Add(float64(len(toInsert)))
}
