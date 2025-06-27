package main

import (
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:   "bodega",
		Action: run,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "clickhouse-addr",
				EnvVars:  []string{"PHOTOCOPY_CLICKHOUSE_ADDR"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "clickhouse-database",
				EnvVars:  []string{"PHOTOCOPY_CLICKHOUSE_DATABASE"},
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
	}

	app.Run(os.Args)
}

var run = func(cmd *cli.Context) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cmd.String("clickhouse-addr")},
		Auth: clickhouse.Auth{
			Database: cmd.String("clickhouse-database"),
			Username: cmd.String("clickhouse-user"),
			Password: cmd.String("clickhouse-pass"),
		},
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	var entries []ClickhousePLCEntry
	if err := conn.Select(cmd.Context, &entries, "SELECT..."); err != nil {
		return err
	}

	return nil
}
