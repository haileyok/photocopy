package photocopy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	inserter "github.com/haileyok/photocopy/internal"
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
	followsInserter *inserter.Inserter
	plcInserter     *inserter.Inserter
}

type Args struct {
	Logger               *slog.Logger
	RelayHost            string
	MetricsAddr          string
	CursorFile           string
	PLCScraperCursorFile string
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

	insertionsHist := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "photocopy_inserts_time",
		Help:    "histogram of photocopy inserts",
		Buckets: prometheus.ExponentialBucketsRange(0.0001, 30, 20),
	}, []string{"type"})

	fi, err := inserter.New(ctx, &inserter.Args{
		PrometheusCounterPrefix: "photocopy_follows",
		Histogram:               insertionsHist,
		BatchSize:               100,
		Logger:                  p.logger,
	})
	if err != nil {
		return nil, err
	}

	is := &Inserters{
		followsInserter: fi,
	}

	p.inserters = is

	plci, err := inserter.New(ctx, &inserter.Args{
		PrometheusCounterPrefix: "photocopy_plc_entries",
		Histogram:               insertionsHist,
		BatchSize:               100,
		Logger:                  args.Logger,
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
