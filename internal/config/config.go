package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

type Config struct {
	Agent        AgentConfig            `yaml:"agent"`
	Output       OutputConfig           `yaml:"output"`
	DefaultTags  map[string]string      `yaml:"default_tags"`
	FileTail     []FileTailConfig       `yaml:"file_tail"`
	System       SystemConfig           `yaml:"system"`
	StatsD       StatsDConfig           `yaml:"statsd"`
	Docker       DockerConfig           `yaml:"docker"`
}

type AgentConfig struct {
	Hostname      string   `yaml:"hostname"`
	BatchSize     int      `yaml:"batch_size"`
	BatchInterval Duration `yaml:"batch_interval"`
	QueueSize     int      `yaml:"queue_size"`
}

type OutputConfig struct {
	URL       string `yaml:"url"`
	Timeout   Duration `yaml:"timeout"`
	RetryMax  int      `yaml:"retry_max"`
}

type FileTailConfig struct {
	Path           string   `yaml:"path"`
	Parser         string   `yaml:"parser"`
	MetricField    string   `yaml:"metric_field"`
	ValueField     string   `yaml:"value_field"`
	TimestampField string   `yaml:"timestamp_field"`
	TagFields      []string `yaml:"tag_fields"`
	Pattern        string   `yaml:"pattern"`
	PollInterval   Duration `yaml:"poll_interval"`
}

type SystemConfig struct {
	Interval       Duration `yaml:"interval"`
	EnabledMetrics []string `yaml:"enabled_metrics"`
}

type StatsDConfig struct {
	Listen  string `yaml:"listen"`
	Enabled bool   `yaml:"enabled"`
}

type DockerConfig struct {
	Enabled  bool     `yaml:"enabled"`
	Interval Duration `yaml:"interval"`
	Endpoint string   `yaml:"endpoint"`
}

func DefaultConfig() *Config {
	return &Config{
		Agent: AgentConfig{
			Hostname:      "",
			BatchSize:     1000,
			BatchInterval: Duration{5 * time.Second},
			QueueSize:     100000,
		},
		Output: OutputConfig{
			URL:      "http://localhost:8080/write",
			Timeout:  Duration{10 * time.Second},
			RetryMax: 3,
		},
		DefaultTags: map[string]string{},
		FileTail:    []FileTailConfig{},
		System: SystemConfig{
			Interval:       Duration{15 * time.Second},
			EnabledMetrics: []string{"cpu", "memory", "disk", "net"},
		},
		StatsD: StatsDConfig{
			Listen:  ":8125",
			Enabled: true,
		},
		Docker: DockerConfig{
			Enabled:  false,
			Interval: Duration{15 * time.Second},
			Endpoint: "unix:///var/run/docker.sock",
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Agent.BatchSize <= 0 {
		cfg.Agent.BatchSize = 1000
	}
	if cfg.Output.RetryMax <= 0 {
		cfg.Output.RetryMax = 3
	}
	return cfg, nil
}
