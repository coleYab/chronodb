package collector

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/coleYab/chronodb/internal/core"
	"github.com/coleYab/chronodb/internal/transform"
)

type FileTailer struct {
	path         string
	parser       transform.Parser
	pollInterval time.Duration
	defaultTags  map[string]string
	stopCh       chan struct{}
}

type FileTailerConfig struct {
	Path         string
	Parser       transform.Parser
	PollInterval time.Duration
	DefaultTags  map[string]string
}

func NewFileTailer(cfg FileTailerConfig) *FileTailer {
	return &FileTailer{
		path:         cfg.Path,
		parser:       cfg.Parser,
		pollInterval: cfg.PollInterval,
		defaultTags:  cfg.DefaultTags,
		stopCh:       make(chan struct{}),
	}
}

func (f *FileTailer) Run(ctx context.Context, out chan<- core.Sample) error {
	if f.pollInterval == 0 {
		f.pollInterval = 1 * time.Second
	}

	var lastSize int64
	var lastIno uint64
	initialSkip := true

	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()

	for {
		newStat, err := os.Stat(f.path)
		if err != nil {
			slog.Error("file tailer stat error", "path", f.path, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-f.stopCh:
				return nil
			case <-ticker.C:
				continue
			}
		}
		newSize := newStat.Size()
		newIno := getIno(newStat)

		if newIno != lastIno || newSize < lastSize {
			lastSize = 0
			lastIno = newIno
		}

		if initialSkip {
			initialSkip = false
			lastSize = newSize
			select {
			case <-ctx.Done():
				return nil
			case <-f.stopCh:
				return nil
			case <-ticker.C:
				continue
			}
		}

		if newSize > lastSize {
			if err := f.readNewLines(ctx, out, lastSize, newSize); err != nil {
				slog.Error("file tailer read error", "path", f.path, "err", err)
			}
			lastSize = newSize
		}

		select {
		case <-ctx.Done():
			return nil
		case <-f.stopCh:
			return nil
		case <-ticker.C:
		}
	}
}

func (f *FileTailer) readNewLines(ctx context.Context, out chan<- core.Sample, offset, end int64) error {
	file, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(offset, 0); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && len(line) == 0 {
			if err == io.EOF {
				return nil
			}
			return err
		}
		line = trimNewline(line)
		if line == "" {
			if err != nil {
				return nil
			}
			continue
		}
		sample, parseErr := f.parser.Parse(line)
		if parseErr != nil {
			slog.Debug("file tailer parse error", "path", f.path, "err", parseErr)
			if err != nil {
				return nil
			}
			continue
		}
		for k, v := range f.defaultTags {
			if _, ok := sample.Tags[k]; !ok {
				sample.Tags[k] = v
			}
		}
		select {
		case out <- *sample:
		case <-ctx.Done():
			return nil
		case <-f.stopCh:
			return nil
		}
		if err != nil {
			return nil
		}
	}
}

func (f *FileTailer) Stop() error {
	close(f.stopCh)
	return nil
}

func getIno(fi os.FileInfo) uint64 {
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		return stat.Ino
	}
	return 0
}

func trimNewline(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	return s
}
