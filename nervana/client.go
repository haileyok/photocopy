package nervana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	cli      *http.Client
	endpoint string
	apiKey   string
}

func NewClient(endpoint string, apiKey string) *Client {
	return &Client{
		cli: &http.Client{
			Timeout: 5 * time.Second,
		},
		endpoint: endpoint,
		apiKey:   apiKey,
	}
}

type NervanaItem struct {
	Text        string `json:"text"`
	Label       string `json:"label"`
	EntityId    string `json:"entityId"`
	Description string `json:"description"`
}

func (c *Client) newRequest(ctx context.Context, text string) (*http.Request, error) {
	payload := map[string]string{
		"text":     text,
		"language": "en",
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(b))

	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	return req, err
}

func (c *Client) MakeRequest(ctx context.Context, text string) ([]NervanaItem, error) {
	req, err := c.newRequest(ctx, text)
	if err != nil {
		return nil, err
	}

	resp, err := c.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("received non-200 response code: %d", resp.StatusCode)
	}

	var nervanaResp []NervanaItem
	if err := json.NewDecoder(resp.Body).Decode(&nervanaResp); err != nil {
		return nil, err
	}

	return nervanaResp, nil
}
