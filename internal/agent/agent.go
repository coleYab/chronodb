package agent

import (
	"context"
	"log/slog"
	"os"

	"github.com/coleYab/chronodb/internal/batcher"
	"github.com/coleYab/chronodb/internal/collector"
	"github.com/coleYab/chronodb/internal/config"
	"github.com/coleYab/chronodb/internal/core"
	"github.com/coleYab/chronodb/internal/shipper"
	"github.com/coleYab/chronodb/internal/transform"
)

type Agent struct {
	cfg        *config.Config
	batcher    *batcher.Batcher
	shipper    *shipper.Shipper
	collectors []collector.Collector
}

func New(cfg *config.Config) *Agent {
	return &Agent{cfg: cfg}
}

func (a *Agent) Run(ctx context.Context) error {
	defaultTags := a.cfg.DefaultTags
	if defaultTags == nil {
		defaultTags = make(map[string]string)
	}
	hostname := a.cfg.Agent.Hostname
	if hostname == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
	}
	defaultTags["host"] = hostname

	a.shipper = shipper.New(shipper.Config{
		URL:      a.cfg.Output.URL,
		Timeout:  a.cfg.Output.Timeout.Duration,
		RetryMax: a.cfg.Output.RetryMax,
	})

	a.batcher = batcher.New(batcher.Config{
		BatchSize:  a.cfg.Agent.BatchSize,
		FlushEvery: a.cfg.Agent.BatchInterval.Duration,
		QueueSize:  a.cfg.Agent.QueueSize,
	})

	if err := a.startCollectors(defaultTags); err != nil {
		return err
	}

	sampleCh := make(chan core.Sample, a.cfg.Agent.QueueSize)
	for _, col := range a.collectors {
		go func(c collector.Collector) {
			if err := c.Run(ctx, sampleCh); err != nil {
				slog.Error("collector error", "err", err)
			}
		}(col)
	}

	go a.batcher.Run(ctx)
	go a.runShipper(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("agent shutting down")
			a.batcher.Flush()
			a.batcher.Wait()
			return nil
		case s := <-sampleCh:
			if err := a.batcher.Submit(s); err != nil {
				slog.Warn("dropping sample", "metric", s.Metric, "err", err)
			}
		}
	}
}

func (a *Agent) runShipper(ctx context.Context) {
	batchCh := a.batcher.BatchCh()
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-batchCh:
			if len(batch) == 0 {
				continue
			}
			if err := a.shipper.Send(ctx, batch); err != nil {
				slog.Error("shipper send error", "samples", len(batch), "err", err)
			}
		}
	}
}

func (a *Agent) startCollectors(defaultTags map[string]string) error {
	capacity := len(a.cfg.FileTail)
	if len(a.cfg.System.EnabledMetrics) > 0 {
		capacity++
	}
	if a.cfg.StatsD.Enabled {
		capacity++
	}
	if a.cfg.Docker.Enabled {
		capacity++
	}
	cols := make([]collector.Collector, 0, capacity)

	for _, ft := range a.cfg.FileTail {
		var parser transform.Parser
		switch ft.Parser {
		case "json":
			parser = transform.NewJSONParser(transform.JSONParserConfig{
				MetricField:    ft.MetricField,
				ValueField:     ft.ValueField,
				TimestampField: ft.TimestampField,
				TagFields:      ft.TagFields,
				DefaultTags:    defaultTags,
			})
		case "regex":
			var err error
			parser, err = transform.NewRegexParser(transform.RegexParserConfig{
				Pattern:     ft.Pattern,
				MetricGroup: ft.MetricField,
				ValueGroup:  ft.ValueField,
				TagGroups:   ft.TagFields,
				DefaultTags: defaultTags,
			})
			if err != nil {
				return err
			}
		default:
			slog.Warn("unknown parser type", "parser", ft.Parser, "path", ft.Path)
			continue
		}
		col := collector.NewFileTailer(collector.FileTailerConfig{
			Path:         ft.Path,
			Parser:       parser,
			PollInterval: ft.PollInterval.Duration,
			DefaultTags:  defaultTags,
		})
		cols = append(cols, col)
		slog.Info("file tail collector started", "path", ft.Path, "parser", ft.Parser, "poll_interval", ft.PollInterval.Duration)
	}

	if len(a.cfg.System.EnabledMetrics) > 0 {
		col := collector.NewSysCollector(collector.SysCollectorConfig{
			Interval:    a.cfg.System.Interval.Duration,
			Enabled:     a.cfg.System.EnabledMetrics,
			DefaultTags: defaultTags,
		})
		cols = append(cols, col)
		slog.Info("system collector started", "interval", a.cfg.System.Interval.Duration, "metrics", a.cfg.System.EnabledMetrics)
	}

	if a.cfg.StatsD.Enabled {
		col := collector.NewStatsDListener(collector.StatsDListenerConfig{
			Addr:        a.cfg.StatsD.Listen,
			DefaultTags: defaultTags,
		})
		cols = append(cols, col)
		slog.Info("statsd listener started", "addr", a.cfg.StatsD.Listen)
	}

	if a.cfg.Docker.Enabled {
		col := collector.NewDockerCollector(collector.DockerCollectorConfig{
			Interval:    a.cfg.Docker.Interval.Duration,
			Endpoint:    a.cfg.Docker.Endpoint,
			DefaultTags: defaultTags,
		})
		cols = append(cols, col)
		slog.Info("docker collector started", "interval", a.cfg.Docker.Interval.Duration)
	}

	a.collectors = cols
	return nil
}
