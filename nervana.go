package photocopy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type NervanaItem struct {
	Text        string `json:"text"`
	Label       string `json:"label"`
	EntityId    string `json:"entityId"`
	Description string `json:"description"`
}

func (p *Photocopy) newNervanaRequest(ctx context.Context, text string) (*http.Request, error) {
	payload := map[string]string{
		"text":     text,
		"language": "en",
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "GET", p.nervanaEndpoint, bytes.NewReader(b))

	req.Header.Set("Authorization", "Bearer "+p.nervanaApiKey)

	return req, err
}

func (p *Photocopy) makeNervanaRequest(ctx context.Context, text string) ([]NervanaItem, error) {
	req, err := p.newNervanaRequest(ctx, text)
	if err != nil {
		return nil, err
	}

	resp, err := p.nervanaClient.Do(req)
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
