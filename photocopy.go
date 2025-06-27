package photocopy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/haileyok/photocopy/clickhouse_inserter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Photocopy struct {
	logger *slog.Logger
	wg     sync.WaitGroup

	relayHost   string
	cursor      string
	cursorFile  string
	metricsAddr string

	inserters *Inserters

	plcScraper *PLCScraper
}

type Inserters struct {
	followsInserter *clickhouse_inserter.Inserter
	plcInserter     *clickhouse_inserter.Inserter
}

type Args struct {
	Logger               *slog.Logger
	RelayHost            string
	MetricsAddr          string
	CursorFile           string
	PLCScraperCursorFile string
	ClickhouseAddr       string
	ClickhouseDatabase   string
	ClickhouseUser       string
	ClickhousePass       string
}

func New(ctx context.Context, args *Args) (*Photocopy, error) {
	p := &Photocopy{
		logger:      args.Logger,
		metricsAddr: args.MetricsAddr,
		relayHost:   args.RelayHost,
		wg:          sync.WaitGroup{},
		cursorFile:  args.CursorFile,
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{args.ClickhouseAddr},
		Auth: clickhouse.Auth{
			Database: args.ClickhouseDatabase,
			Username: args.ClickhouseUser,
			Password: args.ClickhousePass,
		},
	})
	if err != nil {
		return nil, err
	}

	insertionsHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "photocopy_inserts_time",
		Help:    "histogram of photocopy inserts",
		Buckets: prometheus.ExponentialBucketsRange(0.0001, 30, 20),
	}, []string{"type"})

	fi, err := clickhouse_inserter.New(ctx, &clickhouse_inserter.Args{
		PrometheusCounterPrefix: "photocopy_follows",
		Histogram:               insertionsHist,
		BatchSize:               100,
		Logger:                  p.logger,
		Conn:                    conn,
		Query:                   "",
	})
	if err != nil {
		return nil, err
	}

	is := &Inserters{
		followsInserter: fi,
	}

	p.inserters = is

	plci, err := clickhouse_inserter.New(ctx, &clickhouse_inserter.Args{
		PrometheusCounterPrefix: "photocopy_plc_entries",
		Histogram:               insertionsHist,
		BatchSize:               100,
		Logger:                  args.Logger,
		Conn:                    conn,
		Query: `INSERT INTO plc (
			did, cid, nullified, created_at, plc_op_sig, plc_op_prev, plc_op_type,
			plc_op_services, plc_op_also_known_as, plc_op_rotation_keys,
			plc_op_verification_methods, plc_tomb_sig, plc_tomb_prev, plc_tomb_type,
			legacy_op_sig, legacy_op_prev, legacy_op_type, legacy_op_handle,
			legacy_op_service, legacy_op_signing_key, legacy_op_recovery_key
		)`,
	})
	if err != nil {
		return nil, err
	}

	plcs, err := NewPLCScraper(ctx, PLCScraperArgs{
		Logger:     p.logger,
		Inserter:   plci,
		CursorFile: args.PLCScraperCursorFile,
	})
	if err != nil {
		return nil, err
	}

	p.inserters.plcInserter = plci
	p.plcScraper = plcs

	return p, nil
}

func (p *Photocopy) Run(baseCtx context.Context) error {
	ctx, cancel := context.WithCancel(baseCtx)

	metricsServer := http.NewServeMux()
	metricsServer.Handle("/metrics", promhttp.Handler())

	go func() {
		p.logger.Info("Starting metrics server")
		if err := http.ListenAndServe(p.metricsAddr, metricsServer); err != nil {
			p.logger.Error("metrics server failed", "error", err)
		}
	}()

	go func(ctx context.Context, cancel context.CancelFunc) {
		p.logger.Info("starting relay", "relayHost", p.relayHost)
		if err := p.startConsumer(ctx, cancel); err != nil {
			panic(fmt.Errorf("failed to start consumer: %w", err))
		}
	}(ctx, cancel)

	go func(ctx context.Context) {
		if err := p.plcScraper.Run(ctx); err != nil {
			panic(fmt.Errorf("failed to start plc scraper: %w", err))
		}
	}(ctx)

	<-ctx.Done()

	if p.inserters != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

		p.logger.Info("stopping inserters")

		if p.inserters.followsInserter != nil {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				if err := p.inserters.followsInserter.Close(ctx); err != nil {
					p.logger.Error("failed to close follows inserter", "error", err)
					return
				}
				p.logger.Info("follows inserter closed")
			}()
		}

		if p.inserters.plcInserter != nil {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				if err := p.inserters.plcInserter.Close(ctx); err != nil {
					p.logger.Error("failed to close plc inserter", "error", err)
					return
				}
				p.logger.Info("plc inserter closed")
			}()
		}

		p.wg.Wait()

		cancel()

		p.logger.Info("inserters closed")
	}

	return nil
}
