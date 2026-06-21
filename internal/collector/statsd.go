package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

type StatsDListener struct {
	addr        string
	defaultTags map[string]string
	stopCh      chan struct{}
}

type StatsDListenerConfig struct {
	Addr        string
	DefaultTags map[string]string
}

func NewStatsDListener(cfg StatsDListenerConfig) *StatsDListener {
	addr := cfg.Addr
	if addr == "" {
		addr = ":8125"
	}
	return &StatsDListener{
		addr:        addr,
		defaultTags: cfg.DefaultTags,
		stopCh:      make(chan struct{}),
	}
}

func (s *StatsDListener) Run(ctx context.Context, out chan<- core.Sample) error {
	pc, err := net.ListenPacket("udp", s.addr)
	if err != nil {
		return fmt.Errorf("statsd listen: %w", err)
	}
	defer pc.Close()

	buf := make([]byte, 65535)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.stopCh:
			return nil
		default:
			pc.SetReadDeadline(time.Now().Add(time.Second))
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return err
			}
			line := strings.TrimSpace(string(buf[:n]))
			if line == "" {
				continue
			}
			for _, part := range strings.Split(line, "\n") {
				s.handleLine(ctx, out, part)
			}
		}
	}
}

func (s *StatsDListener) Stop() error {
	close(s.stopCh)
	return nil
}

func (s *StatsDListener) handleLine(ctx context.Context, out chan<- core.Sample, line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	sample, err := parseStatsDLine(line, s.defaultTags)
	if err != nil {
		slog.Debug("statsd parse error", "line", line, "err", err)
		return
	}
	select {
	case out <- *sample:
	case <-ctx.Done():
	case <-s.stopCh:
	}
}

func parseStatsDLine(line string, defaultTags map[string]string) (*core.Sample, error) {
	// Format: metric:value|type|@sample_rate|#tag1:val1,tag2:val2
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("no colon")
	}
	metric := parts[0]
	rest := parts[1]

	// Rest: value|type|@rate|#tags
	fields := strings.SplitN(rest, "|", 4)
	if len(fields) < 2 {
		return nil, fmt.Errorf("too few pipe fields")
	}

	valueStr := fields[0]
	metricType := fields[1]

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return nil, fmt.Errorf("parse value %q: %w", valueStr, err)
	}

	rate := 1.0
	tags := make(map[string]string)
	for k, v := range defaultTags {
		tags[k] = v
	}

	for i := 2; i < len(fields); i++ {
		f := fields[i]
		switch {
		case strings.HasPrefix(f, "@"):
			r, err := strconv.ParseFloat(f[1:], 64)
			if err == nil && r > 0 {
				rate = r
			}
		case strings.HasPrefix(f, "#"):
			for _, tag := range strings.Split(f[1:], ",") {
				tag = strings.TrimSpace(tag)
				kv := strings.SplitN(tag, ":", 2)
				if len(kv) == 2 {
					tags[kv[0]] = kv[1]
				} else if len(kv) == 1 && kv[0] != "" {
					tags[kv[0]] = "true"
				}
			}
		}
	}

	var adjusted float64
	switch metricType {
	case "g":
		adjusted = value
	case "c", "ms", "h":
		adjusted = value / rate
	default:
		return nil, fmt.Errorf("unknown type %q", metricType)
	}

	sampleMetric := metric
	if metricType == "ms" || metricType == "h" {
		sampleMetric = metric + "." + metricType
	}

	return &core.Sample{
		Metric:    sampleMetric,
		Tags:      tags,
		Timestamp: time.Now().UnixMilli(),
		Value:     adjusted,
	}, nil
}
