package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

type DockerCollector struct {
	interval    time.Duration
	endpoint    string
	defaultTags map[string]string
	stopCh      chan struct{}
	client      *http.Client
}

type DockerCollectorConfig struct {
	Interval    time.Duration
	Endpoint    string
	DefaultTags map[string]string
}

func NewDockerCollector(cfg DockerCollectorConfig) *DockerCollector {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "unix:///var/run/docker.sock"
	}
	client := httpClientForUnix(endpoint)
	return &DockerCollector{
		interval:    cfg.Interval,
		endpoint:    endpoint,
		defaultTags: cfg.DefaultTags,
		stopCh:      make(chan struct{}),
		client:      client,
	}
}

func httpClientForUnix(endpoint string) *http.Client {
	sockPath := strings.TrimPrefix(endpoint, "unix://")
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}
}

func (d *DockerCollector) Run(ctx context.Context, out chan<- core.Sample) error {
	if d.interval == 0 {
		d.interval = 15 * time.Second
	}

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	// Run once immediately, then on ticker
	for {
		d.collect(ctx, out)

		select {
		case <-ctx.Done():
			return nil
		case <-d.stopCh:
			return nil
		case <-ticker.C:
		}
	}
}

func (d *DockerCollector) Stop() error {
	close(d.stopCh)
	return nil
}

func (d *DockerCollector) collect(ctx context.Context, out chan<- core.Sample) {
	containers, err := d.listContainers(ctx)
	if err != nil {
		slog.Debug("docker list error", "err", err)
		return
	}
	for _, c := range containers {
		stats, err := d.containerStats(ctx, c.ID)
		if err != nil {
			slog.Debug("docker stats error", "container", c.ID, "err", err)
			continue
		}
		d.emitContainerStats(ctx, out, c, stats)
	}
}

func (d *DockerCollector) listContainers(ctx context.Context) ([]dockerContainer, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/containers/json?limit=100", nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var list []dockerContainer
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func (d *DockerCollector) containerStats(ctx context.Context, id string) (*dockerStats, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://localhost/containers/%s/stats?stream=false", id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s dockerStats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (d *DockerCollector) emitContainerStats(ctx context.Context, out chan<- core.Sample, c dockerContainer, s *dockerStats) {
	containerName := c.ID[:12]
	for _, name := range c.Names {
		containerName = strings.TrimPrefix(name, "/")
		if containerName != "" {
			break
		}
	}
	baseTags := map[string]string{
		"container_id":   c.ID[:12],
		"container_name": containerName,
	}

	// CPU
	if s.CPUStats.CPUUsage.TotalUsage > 0 && s.PreCPUStats.CPUUsage.TotalUsage > 0 {
		cpuDelta := s.CPUStats.CPUUsage.TotalUsage - s.PreCPUStats.CPUUsage.TotalUsage
		sysDelta := s.CPUStats.SystemCPUUsage - s.PreCPUStats.SystemCPUUsage
		if sysDelta > 0 {
			pct := 100.0 * float64(cpuDelta) / float64(sysDelta) * float64(s.CPUStats.OnlineCPUs)
			d.emit(ctx, out, "docker.cpu.usage", baseTags, pct)
		}
	}

	// Memory
	if s.MemoryStats.Usage > 0 && s.MemoryStats.Limit > 0 {
		d.emit(ctx, out, "docker.memory.usage", baseTags, float64(s.MemoryStats.Usage))
		d.emit(ctx, out, "docker.memory.limit", baseTags, float64(s.MemoryStats.Limit))
		pct := 100.0 * float64(s.MemoryStats.Usage) / float64(s.MemoryStats.Limit)
		d.emit(ctx, out, "docker.memory.usage_percent", baseTags, pct)
	}

	// Network
	for name, netStats := range s.Networks {
		netTags := copyTags(baseTags, "network", name)
		d.emit(ctx, out, "docker.net.rx_bytes", netTags, float64(netStats.RxBytes))
		d.emit(ctx, out, "docker.net.tx_bytes", netTags, float64(netStats.TxBytes))
		d.emit(ctx, out, "docker.net.rx_packets", netTags, float64(netStats.RxPackets))
		d.emit(ctx, out, "docker.net.tx_packets", netTags, float64(netStats.TxPackets))
	}

	// Block I/O
	var blkRead, blkWrite uint64
	for _, entry := range s.BlkioStats.IOServiceBytesRecursive {
		switch entry.Op {
		case "read":
			blkRead += entry.Value
		case "write":
			blkWrite += entry.Value
		}
	}
	if blkRead > 0 || blkWrite > 0 {
		d.emit(ctx, out, "docker.blkio.read_bytes", baseTags, float64(blkRead))
		d.emit(ctx, out, "docker.blkio.write_bytes", baseTags, float64(blkWrite))
	}
}

func (d *DockerCollector) emit(ctx context.Context, out chan<- core.Sample, metric string, extraTags map[string]string, value float64) {
	tags := make(map[string]string, len(d.defaultTags)+len(extraTags))
	for k, v := range d.defaultTags {
		tags[k] = v
	}
	for k, v := range extraTags {
		tags[k] = v
	}
	select {
	case out <- core.Sample{Metric: metric, Tags: tags, Timestamp: time.Now().UnixMilli(), Value: value}:
	case <-ctx.Done():
	case <-d.stopCh:
	}
}

type dockerContainer struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
	State string   `json:"State"`
}

type dockerStats struct {
	CPUStats    cpuStats    `json:"cpu_stats"`
	PreCPUStats cpuStats    `json:"precpu_stats"`
	MemoryStats memoryStats `json:"memory_stats"`
	Networks    map[string]netStats `json:"networks"`
	BlkioStats  blkioStats  `json:"blkio_stats"`
}

type cpuStats struct {
	CPUUsage       cpuUsage `json:"cpu_usage"`
	SystemCPUUsage uint64   `json:"system_cpu_usage"`
	OnlineCPUs     uint64   `json:"online_cpus"`
}

type cpuUsage struct {
	TotalUsage uint64 `json:"total_usage"`
}

type memoryStats struct {
	Usage uint64 `json:"usage"`
	Limit uint64 `json:"limit"`
}

type netStats struct {
	RxBytes   uint64 `json:"rx_bytes"`
	RxPackets uint64 `json:"rx_packets"`
	TxBytes   uint64 `json:"tx_bytes"`
	TxPackets uint64 `json:"tx_packets"`
}

type blkioStats struct {
	IOServiceBytesRecursive []blkioEntry `json:"io_service_bytes_recursive"`
}

type blkioEntry struct {
	Op    string `json:"op"`
	Value uint64 `json:"value"`
}

func copyTags(orig map[string]string, extraKey, extraVal string) map[string]string {
	m := make(map[string]string, len(orig)+1)
	for k, v := range orig {
		m[k] = v
	}
	m[extraKey] = extraVal
	return m
}
