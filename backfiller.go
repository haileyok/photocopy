package photocopy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	atproto_repo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	_ "github.com/joho/godotenv/autoload"
	"go.uber.org/ratelimit"
)

type RepoDownloader struct {
	clients    map[string]*http.Client
	rateLimits map[string]ratelimit.Limiter
	mu         sync.RWMutex
	p          *Photocopy
}

func NewRepoDownloader(p *Photocopy) *RepoDownloader {
	return &RepoDownloader{
		clients:    make(map[string]*http.Client),
		rateLimits: make(map[string]ratelimit.Limiter),
		p:          p,
	}
}

func (rd *RepoDownloader) getClient(service string) *http.Client {
	rd.mu.RLock()
	client, exists := rd.clients[service]
	rd.mu.RUnlock()

	if exists {
		return client
	}

	rd.mu.Lock()
	defer rd.mu.Unlock()

	if client, exists := rd.clients[service]; exists {
		return client
	}

	client = util.RobustHTTPClient()
	client.Timeout = 45 * time.Second
	rd.clients[service] = client
	return client
}

func (rd *RepoDownloader) getRateLimiter(service string) ratelimit.Limiter {
	if !strings.HasSuffix(service, ".bsky.network") {
		service = "third-party"
	}

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

	if rd.p.ratelimitBypassKey != "" && strings.HasSuffix(service, ".bsky.network") {
		req.Header.Set("x-ratelimit-bypass", rd.p.ratelimitBypassKey)
	}

	client := rd.getClient(service)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download repo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 400 {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read bytes from response: %w", err)
	}

	return b, nil
}

func (p *Photocopy) processRepo(ctx context.Context, b []byte, did string) error {
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
		if err := p.handleCreate(ctx, b.RawData(), time.Now().Format(time.RFC3339Nano), "unk", did, nsid, rkey, cidStr, "unk"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("erorr traversing records: %v", err)
	}

	return nil
}

type ListReposResponse struct {
	Cursor string          `json:"cursor"`
	Repos  []ListReposRepo `json:"repos"`
}

type ListReposRepo struct {
	Did    string  `json:"did"`
	Head   string  `json:"head"`
	Rev    string  `json:"rev"`
	Active bool    `json:"active"`
	Status *string `json:"status,omitempty"`
}

func (rd *RepoDownloader) getDidsFromService(ctx context.Context, service string) ([]ListReposRepo, error) {
	var cursor string
	var repos []ListReposRepo
	if service == "https://atproto.brid.gy" {
		return nil, nil
	}
	for {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/xrpc/com.atproto.sync.listRepos?limit=1000&cursor=%s", service, cursor), nil)
		if err != nil {
			return nil, err
		}

		if rd.p.ratelimitBypassKey != "" && strings.HasSuffix(service, ".bsky.network") {
			req.Header.Set("x-ratelimit-bypass", rd.p.ratelimitBypassKey)
		}

		rl := rd.getRateLimiter(service)
		rl.Take()

		cli := rd.getClient(service)
		resp, err := cli.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("received non-200 response code: %d", resp.StatusCode)
		}

		var reposResp ListReposResponse
		if err := json.NewDecoder(resp.Body).Decode(&reposResp); err != nil {
			return nil, fmt.Errorf("error decoding repos response: %w", err)
		}

		for _, repo := range reposResp.Repos {
			if repo.Status != nil {
				if *repo.Status == "deleted" || *repo.Status == "takendown" || *repo.Status == "deactivated" {
					continue
				}
			}

			repos = append(repos, repo)
		}

		if len(reposResp.Repos) != 1000 || reposResp.Cursor == "" {
			break
		}

		fmt.Printf("cursor %s service %s\n", reposResp.Cursor, service)

		cursor = reposResp.Cursor
	}

	return repos, nil
}

type ListServicesResponse struct {
	Cursor string                     `json:"cursor"`
	Hosts  []ListServicesResponseItem `json:"hosts"`
}

type ListServicesResponseItem struct {
	Hostname string `json:"hostname"`
	Status   string `json:"status"`
}

func (p *Photocopy) runBackfiller(ctx context.Context) error {
	startTime := time.Now()

	fmt.Println("querying clickhouse for dids and services...")

	var hostsCursor string
	var sevs []ListServicesResponseItem
	for {
		if hostsCursor != "" {
			hostsCursor = "&cursor=" + hostsCursor
		}
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://relay1.us-east.bsky.network/xrpc/com.atproto.sync.listHosts?limit=1000%s", hostsCursor), nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("received non-200 response code: %d", resp.StatusCode)
		}

		var sevsResp ListServicesResponse
		if err := json.NewDecoder(resp.Body).Decode(&sevsResp); err != nil {
			return fmt.Errorf("error decoding sevs response: %w", err)
		}

		for _, sev := range sevsResp.Hosts {
			if sev.Status != "active" {
				continue
			}

			sevs = append(sevs, sev)
		}

		if len(sevsResp.Hosts) != 1000 || sevsResp.Cursor == "" {
			break
		}

		hostsCursor = sevsResp.Cursor
	}

	servicesDids := map[string][]string{}
	for _, sev := range sevs {
		servicesDids["https://"+sev.Hostname] = []string{}
	}

	fmt.Printf("found %d services\n", len(servicesDids))

	fmt.Printf("collecting dids...\n")

	fmt.Printf("building download buckets...")

	skipped := 0
	downloader := NewRepoDownloader(p)
	serviceDids := map[string][]string{}

	wg := sync.WaitGroup{}
	mplk := sync.Mutex{}
	for s := range servicesDids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			repos, err := downloader.getDidsFromService(context.TODO(), s)
			if err != nil {
				fmt.Printf("error getting dids for services %s: %v", s, err)
				return
			}
			dids := []string{}
			for _, r := range repos {
				dids = append(dids, r.Did)
			}
			mplk.Lock()
			defer mplk.Unlock()
			serviceDids[s] = dids
		}()
	}

	fmt.Println("getting all the repos...")
	wg.Wait()

	fmt.Printf("was able to skip %d repos\n", skipped)

	total := 0

	for service, dids := range serviceDids {
		if len(dids) < 100 {
			continue
		}
		fmt.Printf("%s: %d jobs\n", service, len(dids))
		total += len(dids)
	}

	fmt.Printf("Total jobs: %d across %d services \n", total, len(serviceDids))

	for _, c := range downloader.clients {
		c.Timeout = 10 * time.Minute
	}

	for s := range downloader.rateLimits {
		if p.ratelimitBypassKey != "" && strings.HasSuffix(s, ".bsky.network") {
			downloader.rateLimits[s] = ratelimit.New(25)
		}
	}

	processed := 0
	errored := 0
	var errors []error
	for service, dids := range serviceDids {
		go func() {
			for _, did := range dids {
				ratelimiter := downloader.getRateLimiter(service)
				ratelimiter.Take()

				b, err := downloader.downloadRepo(service, did)
				if err != nil {
					errored++
					processed++
					errors = append(errors, err)
					continue
				}

				go func(b []byte, did string) {
					if err := p.processRepo(ctx, b, did); err != nil {
						fmt.Printf("error processing backfill record: %v\n", err)
					}
				}(b, did)

				processed++
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		elapsed := time.Since(startTime)
		rate := float64(processed) / elapsed.Seconds()
		remaining := total - processed

		var eta string
		if rate > 0 {
			etaSeconds := float64(remaining) / rate
			etaDuration := time.Duration(etaSeconds * float64(time.Second))
			eta = fmt.Sprintf(", ETA: %v", etaDuration.Round(time.Second))
		} else {
			eta = ", ETA: calculating..."
		}

		for _, err := range errors {
			fmt.Printf("%v\n", err)
		}

		errors = nil

		fmt.Printf("\rProgress: %d/%d processed (%.1f%%), %d skipped, %d errors, %.1f jobs/sec%s",
			processed, total, float64(processed)/float64(total)*100, skipped, errored, rate, eta)
	}

	fmt.Printf("\nCompleted: %d processed, %d errors\n", processed, errored)

	return nil
}
