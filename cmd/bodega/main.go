package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	atproto_repo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/haileyok/photocopy/clickhouse_inserter"
	"github.com/haileyok/photocopy/models"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
	"go.uber.org/ratelimit"
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
			&cli.BoolFlag{
				Name:  "debug",
				Value: false,
			},
		},
	}

	app.Run(os.Args)
}

type RepoDownloader struct {
	client     *http.Client
	rateLimits map[string]ratelimit.Limiter
	mu         sync.RWMutex
}

func NewRepoDownloader() *RepoDownloader {
	return &RepoDownloader{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        5000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     90 * time.Second,
				DisableKeepAlives:   false,
				MaxConnsPerHost:     1000,
			},
			Timeout: 30 * time.Second,
		},
		rateLimits: make(map[string]ratelimit.Limiter),
	}
}

func (rd *RepoDownloader) getRateLimiter(service string) ratelimit.Limiter {
	rd.mu.RLock()
	limiter, exists := rd.rateLimits[service]
	rd.mu.RUnlock()

	if exists {
		return limiter
	}

	rd.mu.Lock()
	defer rd.mu.Unlock()

	if limiter, exists := rd.rateLimits[service]; exists {
		return limiter
	}

	// 3000 per five minutes
	limiter = ratelimit.New(10)
	rd.rateLimits[service] = limiter
	return limiter
}

func (rd *RepoDownloader) downloadRepo(service, did string) ([]byte, error) {
	dlurl := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", service, did)

	req, err := http.NewRequestWithContext(context.TODO(), "GET", dlurl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := rd.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download repo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read bytes from response: %w", err)
	}

	return b, nil
}

func processRepo(b []byte, did string, inserter *clickhouse_inserter.Inserter) error {
	bs := atproto_repo.NewTinyBlockstore()
	cs, err := car.NewCarReader(bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("error opening car: %v\n", err)
	}

	currBlock, _ := cs.Next()
	for currBlock != nil {
		bs.Put(context.TODO(), currBlock)
		next, _ := cs.Next()
		currBlock = next
	}

	r, err := repo.OpenRepo(context.TODO(), bs, cs.Header.Roots[0])
	if err != nil || r == nil {
		fmt.Printf("could not open repo: %v", err)
		return nil
	}

	if err := r.ForEach(context.TODO(), "", func(key string, cid cid.Cid) error {
		pts := strings.Split(key, "/")
		nsid := pts[0]
		rkey := pts[1]
		cidStr := cid.String()
		b, err := bs.Get(context.TODO(), cid)
		if err != nil {
			return nil
		}

		var cat time.Time
		tid, err := syntax.ParseTID(rkey)
		if err != nil {
			cat = time.Now()
		} else {
			cat = tid.Time()
		}

		rec := models.Record{
			Did:        did,
			Rkey:       rkey,
			Collection: nsid,
			Cid:        cidStr,
			Seq:        "",
			Raw:        b.RawData(),
			CreatedAt:  cat,
		}

		inserter.Insert(context.TODO(), rec)

		return nil
	}); err != nil {
		return fmt.Errorf("erorr traversing records: %v", err)
	}

	return nil
}

var run = func(cmd *cli.Context) error {
	startTime := time.Now()

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

	fmt.Println("querying clickhouse for dids and services...")

	var entries []ClickhousePLCEntry
	if err := conn.Select(cmd.Context, &entries, `
SELECT did, plc.plc_op_services
FROM default.plc
WHERE has(plc_op_services, 'https://pds.trump.com') = 0 -- trump pds spam
AND has(plc_op_services, 'https://plc.surge.sh/gallery') = 0 -- plc.surge.sh spam
AND arrayExists(x -> x LIKE 'https://a.co/%', plc_op_services) = 0 -- more plc.surge.sh spam
QUALIFY row_number() OVER (PARTITION BY did ORDER BY created_at DESC) = 1
		`); err != nil {
		return err
	}

	fmt.Printf("found %d entries", len(entries))

	inserter, err := clickhouse_inserter.New(context.TODO(), &clickhouse_inserter.Args{
		BatchSize: 100000,
		Logger:    slog.Default(),
		Conn:      conn,
		Query:     "INSERT INTO record (did, rkey, collection, cid, seq, raw, created_at)",
		RateLimit: 2, // two inserts per second in the event of massive repos
	})
	if err != nil {
		return err
	}

	fmt.Printf("building download buckets...")

	downloader := NewRepoDownloader()

	serviceDids := map[string][]string{}
	for _, e := range entries {
		if len(e.PlcOpServices) == 0 {
			continue
		}

		for _, s := range e.PlcOpServices {
			if _, err := url.Parse(s); err != nil {
				continue
			}
			serviceDids[s] = append(serviceDids[s], e.Did)
		}
	}

	fmt.Printf("Total jobs: %d across %d services \n", len(entries), len(serviceDids))

	for service, dids := range serviceDids {
		if len(dids) < 100 {
			continue
		}
		fmt.Printf("%s: %d jobs\n", service, len(dids))
	}

	processed := 0
	errored := 0
	for service, dids := range serviceDids {
		go func() {
			for _, did := range dids {
				ratelimiter := downloader.getRateLimiter(service)
				ratelimiter.Take()

				b, err := downloader.downloadRepo(service, did)
				if err != nil {
					continue
				}

				go func(b []byte, did string, inserter *clickhouse_inserter.Inserter) {
					processRepo(b, did, inserter)
				}(b, did, inserter)
				processed++
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		elapsed := time.Since(startTime)
		rate := float64(processed) / elapsed.Seconds()
		remaining := len(entries) - processed

		var eta string
		if rate > 0 {
			etaSeconds := float64(remaining) / rate
			etaDuration := time.Duration(etaSeconds * float64(time.Second))
			eta = fmt.Sprintf(", ETA: %v", etaDuration.Round(time.Second))
		} else {
			eta = ", ETA: calculating..."
		}

		fmt.Printf("\rProgress: %d/%d processed (%.1f%%), %d errors, %.1f jobs/sec%s",
			processed, len(entries), float64(processed)/float64(len(entries))*100, errored, rate, eta)
	}

	fmt.Printf("\nCompleted: %d processed, %d errors\n", processed, errored)

	inserter.Close(context.TODO())

	return nil
}
