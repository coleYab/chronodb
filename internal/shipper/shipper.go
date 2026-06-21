package shipper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

type Shipper struct {
	url      string
	client   *http.Client
	retryMax int
}

type Config struct {
	URL      string
	Timeout  time.Duration
	RetryMax int
}

func New(cfg Config) *Shipper {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 3
	}
	return &Shipper{
		url:      cfg.URL,
		retryMax: cfg.RetryMax,
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:    10,
				IdleConnTimeout: 30 * time.Second,
			},
		},
	}
}

type seriesGroup struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Points []point           `json:"points"`
}

type point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type writeRequest struct {
	Series []seriesGroup `json:"series"`
}

func (s *Shipper) Send(ctx context.Context, batch []core.Sample) error {
	if len(batch) == 0 {
		return nil
	}
	req := s.buildRequest(batch)
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt <= s.retryMax; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}
		lastErr = s.post(ctx, body)
		if lastErr == nil {
			return nil
		}
		slog.Warn("shipper attempt failed", "attempt", attempt+1, "err", lastErr)
	}
	return fmt.Errorf("shipper failed after %d retries: %w", s.retryMax, lastErr)
}

func (s *Shipper) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func (s *Shipper) buildRequest(batch []core.Sample) writeRequest {
	groups := make(map[string]*seriesGroup)
	keys := make([]string, 0)

	for _, sample := range batch {
		key := seriesKey(sample.Metric, sample.Tags)
		g, ok := groups[key]
		if !ok {
			tags := make(map[string]string, len(sample.Tags))
			for k, v := range sample.Tags {
				tags[k] = v
			}
			g = &seriesGroup{
				Metric: sample.Metric,
				Tags:   tags,
				Points: make([]point, 0, 1),
			}
			groups[key] = g
			keys = append(keys, key)
		}
		g.Points = append(g.Points, point{
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		})
	}

	sort.Strings(keys)
	series := make([]seriesGroup, 0, len(keys))
	for _, k := range keys {
		series = append(series, *groups[k])
	}
	return writeRequest{Series: series}
}

func seriesKey(metric string, tags map[string]string) string {
	if len(tags) == 0 {
		return metric + "|"
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	key := metric
	for _, k := range keys {
		key += "|" + k + "=" + tags[k]
	}
	return key
}

func backoff(attempt int) time.Duration {
	return time.Duration(50*attempt) * time.Millisecond
}
