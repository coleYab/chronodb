package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

type SysCollector struct {
	interval    time.Duration
	enabled     []string
	defaultTags map[string]string
	stopCh      chan struct{}

	prevCPUTotal uint64
	prevCPUIdle  uint64
	prevDisk     map[string]diskSnapshot
	prevNet      map[string]netSnapshot
	first        bool
}

type diskSnapshot struct {
	reads  uint64
	writes uint64
	rbytes uint64
	wbytes uint64
}

type netSnapshot struct {
	rxBytes   uint64
	txBytes   uint64
	rxPackets uint64
	txPackets uint64
}

type SysCollectorConfig struct {
	Interval    time.Duration
	Enabled     []string
	DefaultTags map[string]string
}

func NewSysCollector(cfg SysCollectorConfig) *SysCollector {
	enabled := cfg.Enabled
	if len(enabled) == 0 {
		enabled = []string{"cpu", "memory", "disk", "net"}
	}
	return &SysCollector{
		interval:    cfg.Interval,
		enabled:     enabled,
		defaultTags: cfg.DefaultTags,
		stopCh:      make(chan struct{}),
		prevDisk:    make(map[string]diskSnapshot),
		prevNet:     make(map[string]netSnapshot),
		first:       true,
	}
}

func (s *SysCollector) Run(ctx context.Context, out chan<- core.Sample) error {
	if s.interval == 0 {
		s.interval = 15 * time.Second
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		collectAll := true
		for _, name := range s.enabled {
			switch name {
			case "cpu":
				s.collectCPU(ctx, out, collectAll)
			case "memory":
				s.collectMemory(ctx, out)
			case "disk":
				s.collectDisk(ctx, out, collectAll)
			case "net":
				s.collectNet(ctx, out, collectAll)
			}
		}
		s.first = false

		select {
		case <-ctx.Done():
			return nil
		case <-s.stopCh:
			return nil
		case <-ticker.C:
		}
	}
}

func (s *SysCollector) Stop() error {
	close(s.stopCh)
	return nil
}

func (s *SysCollector) collectCPU(ctx context.Context, out chan<- core.Sample, _ bool) {
	total, idle, err := readCPU()
	if err != nil {
		slog.Debug("syscollector cpu error", "err", err)
		return
	}
	if s.first {
		s.prevCPUTotal = total
		s.prevCPUIdle = idle
		return
	}
	dt := total - s.prevCPUTotal
	di := idle - s.prevCPUIdle
	s.prevCPUTotal = total
	s.prevCPUIdle = idle
	if dt == 0 {
		return
	}
	pct := 100.0 * float64(dt-di) / float64(dt)
	s.emit(ctx, out, "system.cpu.usage", nil, pct)
}

func (s *SysCollector) collectMemory(ctx context.Context, out chan<- core.Sample) {
	total, avail, err := readMemory()
	if err != nil {
		slog.Debug("syscollector memory error", "err", err)
		return
	}
	if total == 0 {
		return
	}
	used := total - avail
	pct := 100.0 * float64(used) / float64(total)
	s.emit(ctx, out, "system.memory.total", nil, float64(total))
	s.emit(ctx, out, "system.memory.available", nil, float64(avail))
	s.emit(ctx, out, "system.memory.used", nil, float64(used))
	s.emit(ctx, out, "system.memory.used_percent", nil, pct)
}

func (s *SysCollector) collectDisk(ctx context.Context, out chan<- core.Sample, _ bool) {
	disks, err := readDiskStats()
	if err != nil {
		slog.Debug("syscollector disk error", "err", err)
		return
	}
	for name, cur := range disks {
		prev, ok := s.prevDisk[name]
		if !ok || s.first {
			s.prevDisk[name] = cur
			continue
		}
		s.emit(ctx, out, "system.disk.reads", map[string]string{"device": name}, float64(cur.reads-prev.reads))
		s.emit(ctx, out, "system.disk.writes", map[string]string{"device": name}, float64(cur.writes-prev.writes))
		s.emit(ctx, out, "system.disk.read_bytes", map[string]string{"device": name}, float64(cur.rbytes-prev.rbytes))
		s.emit(ctx, out, "system.disk.write_bytes", map[string]string{"device": name}, float64(cur.wbytes-prev.wbytes))
		s.prevDisk[name] = cur
	}
}

func (s *SysCollector) collectNet(ctx context.Context, out chan<- core.Sample, _ bool) {
	ifaces, err := readNetStats()
	if err != nil {
		slog.Debug("syscollector net error", "err", err)
		return
	}
	for name, cur := range ifaces {
		prev, ok := s.prevNet[name]
		if !ok || s.first {
			s.prevNet[name] = cur
			continue
		}
		s.emit(ctx, out, "system.net.rx_bytes", map[string]string{"interface": name}, float64(cur.rxBytes-prev.rxBytes))
		s.emit(ctx, out, "system.net.tx_bytes", map[string]string{"interface": name}, float64(cur.txBytes-prev.txBytes))
		s.emit(ctx, out, "system.net.rx_packets", map[string]string{"interface": name}, float64(cur.rxPackets-prev.rxPackets))
		s.emit(ctx, out, "system.net.tx_packets", map[string]string{"interface": name}, float64(cur.txPackets-prev.txPackets))
		s.prevNet[name] = cur
	}
}

func (s *SysCollector) emit(ctx context.Context, out chan<- core.Sample, metric string, extraTags map[string]string, value float64) {
	tags := make(map[string]string, len(s.defaultTags)+len(extraTags))
	for k, v := range s.defaultTags {
		tags[k] = v
	}
	for k, v := range extraTags {
		tags[k] = v
	}
	select {
	case out <- core.Sample{Metric: metric, Tags: tags, Timestamp: time.Now().UnixMilli(), Value: value}:
	case <-ctx.Done():
	case <-s.stopCh:
	}
}

func readCPU() (total, idle uint64, err error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	if !scanner.Scan() {
		return 0, 0, fmt.Errorf("empty /proc/stat")
	}
	line := scanner.Text()
	if !strings.HasPrefix(line, "cpu ") {
		return 0, 0, fmt.Errorf("unexpected /proc/stat format")
	}
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return 0, 0, fmt.Errorf("too few cpu fields")
	}
	var vals [8]uint64
	for i := 1; i < len(fields) && i < 9; i++ {
		v, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		vals[i-1] = v
	}
	// user nice system idle iowait irq softirq steal
	total = vals[0] + vals[1] + vals[2] + vals[3] + vals[4] + vals[5] + vals[6] + vals[7]
	idle = vals[3] + vals[4]
	return total, idle, nil
}

func readMemory() (total, available uint64, err error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "MemTotal:"):
			total = parseMemValue(line)
		case strings.HasPrefix(line, "MemAvailable:"):
			available = parseMemValue(line)
		}
	}
	if total == 0 {
		return 0, 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
	}
	return total * 1024, available * 1024, nil
}

func parseMemValue(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[1], 10, 64)
	return v
}

func readDiskStats() (map[string]diskSnapshot, error) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	result := make(map[string]diskSnapshot)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if isPhysicalDisk(name) {
			ri, _ := strconv.ParseUint(fields[3], 10, 64)
			wi, _ := strconv.ParseUint(fields[7], 10, 64)
			rs, _ := strconv.ParseUint(fields[5], 10, 64)
			ws, _ := strconv.ParseUint(fields[9], 10, 64)
			result[name] = diskSnapshot{
				reads:  ri,
				writes: wi,
				rbytes: rs * 512,
				wbytes: ws * 512,
			}
		}
	}
	return result, nil
}

func isPhysicalDisk(name string) bool {
	if name == "" || name[0] == ' ' {
		return false
	}
	// Skip partitions (have digits at end), loop, ram, dm
	for _, skip := range []string{"loop", "ram", "dm-"} {
		if strings.HasPrefix(name, skip) {
			return false
		}
	}
	// Accept sdX, nvmeXnY, vdX, xvdX
	return true
}

func readNetStats() (map[string]netSnapshot, error) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return nil, err
	}
	result := make(map[string]netSnapshot)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		colonIdx := strings.Index(line, ":")
		if colonIdx < 0 {
			continue
		}
		name := strings.TrimSpace(line[:colonIdx])
		rest := strings.Fields(line[colonIdx+1:])
		if len(rest) < 10 {
			continue
		}
		rxb, _ := strconv.ParseUint(rest[0], 10, 64)
		rxp, _ := strconv.ParseUint(rest[1], 10, 64)
		txb, _ := strconv.ParseUint(rest[8], 10, 64)
		txp, _ := strconv.ParseUint(rest[9], 10, 64)
		result[name] = netSnapshot{
			rxBytes:   rxb,
			txBytes:   txb,
			rxPackets: rxp,
			txPackets: txp,
		}
	}
	return result, nil
}
