package photocopy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/haileyok/photocopy/clickhouse_inserter"
)

type PLCScraper struct {
	client     *http.Client
	logger     *slog.Logger
	cursor     string
	cursorFile string
	inserter   *clickhouse_inserter.Inserter
}

type PLCScraperArgs struct {
	Logger     *slog.Logger
	Inserter   *clickhouse_inserter.Inserter
	CursorFile string
}

func NewPLCScraper(ctx context.Context, args PLCScraperArgs) (*PLCScraper, error) {
	if args.Logger == nil {
		args.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	cli := &http.Client{
		Timeout: 15 * time.Second,
	}

	return &PLCScraper{
		client:     cli,
		logger:     args.Logger,
		inserter:   args.Inserter,
		cursorFile: args.CursorFile,
	}, nil
}

func (s *PLCScraper) Run(ctx context.Context) error {
	startCursor, err := s.getCursor()
	if err != nil {
		s.logger.Error("error getting cursor", "error", err)
	}
	s.cursor = startCursor

	ticker := time.NewTicker(3 * time.Second)
	currTickerDuration := 3 * time.Second

	setTickerDuration := func(d time.Duration) {
		if currTickerDuration == d {
			return
		}
		ticker.Reset(d)
		currTickerDuration = d
	}

	for range ticker.C {
		s.logger.Info("performing scrape", "cursor", s.cursor)

		ustr := "https://plc.directory/export?limit=1000"
		if s.cursor != "" {
			ustr += "&after=" + s.cursor
			t, _ := time.Parse(time.RFC3339Nano, s.cursor)
			if time.Since(t) > 1*time.Hour {
				setTickerDuration(800 * time.Millisecond)
			} else {
				setTickerDuration(3 * time.Second)
			}
		}

		req, err := http.NewRequestWithContext(ctx, "GET", ustr, nil)
		if err != nil {
			s.logger.Error("error creating request", "error", err)
			continue
		}

		resp, err := s.client.Do(req)
		if err != nil {
			s.logger.Error("error getting response", "error", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			s.logger.Error("export returned non-200 status", "status", resp.StatusCode)
			continue
		}

		b, err := io.ReadAll(resp.Body)
		if err != nil {
			s.logger.Error("error reading response body", "error", err)
			continue
		}

		rawEntries := strings.Split(string(b), "\n")

		for i, rawEntry := range rawEntries {
			if rawEntry == "" {
				continue
			}

			var entry PLCEntry
			if err := json.Unmarshal([]byte(rawEntry), &entry); err != nil {
				s.logger.Error("error unmarshaling entry", "error", err)
				continue
			}

			// stop inserting if context is cancelled
			if ctx.Err() != nil {
				ticker.Stop()
				break
			}

			if i == len(rawEntries)-1 {
				s.cursor = entry.CreatedAt.Format(time.RFC3339Nano)
				// TODO: this should checking if the currently saved cursor is older than what is already saved
				s.saveCursor(s.cursor)
			}

			chEntry, err := entry.prepareForClickhouse()
			if err != nil {
				s.logger.Error("error getting clickhouse entry from plc entry", "error", err)
				continue
			}

			s.inserter.Insert(ctx, *chEntry)
		}
	}

	return nil
}

func (s *PLCScraper) getCursor() (string, error) {
	cursor, err := os.ReadFile(s.cursorFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}

		return "", fmt.Errorf("failed to read cursor: %w", err)
	}
	return string(cursor), nil
}

func (s *PLCScraper) saveCursor(cursor string) error {
	if err := os.WriteFile(s.cursorFile, []byte(cursor), 0644); err != nil {
		return fmt.Errorf("failed to save cursor: %w", err)
	}
	return nil
}
