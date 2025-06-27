package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/haileyok/photocopy"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:   "photocopy",
		Usage:  "bigquery inserter for firehose events",
		Action: run,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "relay-host",
				EnvVars: []string{"PHOTOCOPY_RELAY_HOST"},
				Value:   "wss://bsky.network",
			},
			&cli.StringFlag{
				Name:    "metrics-addr",
				EnvVars: []string{"PHOTOCOPY_METRICS_ADDR"},
				Value:   ":8000",
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"PHOTOCOPY_LOG_LEVEL"},
				Value:   "info",
			},
			&cli.StringFlag{
				Name:     "cursor-file",
				EnvVars:  []string{"PHOTOCOPY_CURSOR_FILE"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "plc-scraper-cursor-file",
				EnvVars:  []string{"PHOTOCOPY_PLC_SCRAPER_CURSOR_FILE"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "clickhouse-user",
				EnvVars: []string{"PHOTOCOPY_CLICKHOUSE_USER"},
				Value:   "default",
			},
			&cli.StringFlag{
				Name:     "clickhouse-pass",
				EnvVars:  []string{"PHOTOCOPY_CLICKHOUSE_PASS"},
				Required: true,
			},
		},
		ErrWriter: os.Stderr,
	}

	app.Run(os.Args)
}

var run = func(cmd *cli.Context) error {
	ctx := cmd.Context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var level slog.Level
	switch cmd.String("log-level") {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	p, err := photocopy.New(ctx, &photocopy.Args{
		Logger:               l,
		RelayHost:            cmd.String("relay-host"),
		MetricsAddr:          cmd.String("metrics-addr"),
		CursorFile:           cmd.String("cursor-file"),
		PLCScraperCursorFile: cmd.String("plc-scraper-cursor-file"),
		ClickhouseUser:       cmd.String("clickhouse-user"),
		ClickhousePass:       cmd.String("clickhouse-pass"),
	})
	if err != nil {
		panic(err)
	}

	go func() {
		exitSignals := make(chan os.Signal, 1)
		signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)

		sig := <-exitSignals

		l.Info("received os exit signal", "signal", sig)
	}()

	if err := p.Run(ctx); err != nil {
		return err
	}

	return nil
}
