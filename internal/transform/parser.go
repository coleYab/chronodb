package transform

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coleYab/chronodb/internal/core"
)

type Parser interface {
	Parse(line string) (*core.Sample, error)
}

type JSONParser struct {
	MetricField    string
	ValueField     string
	TimestampField string
	TagFields      []string
	DefaultTags    map[string]string
}

func NewJSONParser(cfg JSONParserConfig) *JSONParser {
	return &JSONParser{
		MetricField:    cfg.MetricField,
		ValueField:     cfg.ValueField,
		TimestampField: cfg.TimestampField,
		TagFields:      cfg.TagFields,
		DefaultTags:    cfg.DefaultTags,
	}
}

type JSONParserConfig struct {
	MetricField    string
	ValueField     string
	TimestampField string
	TagFields      []string
	DefaultTags    map[string]string
}

func (p *JSONParser) Parse(line string) (*core.Sample, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		return nil, fmt.Errorf("json parse: %w", err)
	}
	return p.extractSample(data)
}

func (p *JSONParser) extractSample(data map[string]interface{}) (*core.Sample, error) {
	s := &core.Sample{
		Tags: make(map[string]string),
	}
	for k, v := range p.DefaultTags {
		s.Tags[k] = v
	}
	metricRaw, ok := data[p.MetricField]
	if !ok {
		return nil, fmt.Errorf("metric field %q not found", p.MetricField)
	}
	s.Metric = fmt.Sprintf("%v", metricRaw)
	valueRaw, ok := data[p.ValueField]
	if !ok {
		return nil, fmt.Errorf("value field %q not found", p.ValueField)
	}
	v, err := toFloat64(valueRaw)
	if err != nil {
		return nil, fmt.Errorf("value field %q: %w", p.ValueField, err)
	}
	s.Value = v
	if p.TimestampField != "" {
		tsRaw, ok := data[p.TimestampField]
		if ok {
			ts, err := toInt64(tsRaw)
			if err != nil {
				return nil, fmt.Errorf("timestamp field %q: %w", p.TimestampField, err)
			}
			s.Timestamp = ts
		}
	}
	if s.Timestamp == 0 {
		s.Timestamp = time.Now().UnixMilli()
	}
	for _, field := range p.TagFields {
		val, ok := data[field]
		if ok {
			s.Tags[field] = fmt.Sprintf("%v", val)
		}
	}
	return s, nil
}

type RegexParser struct {
	Pattern     *regexp.Regexp
	MetricGroup string
	ValueGroup  string
	TagGroups   []string
	DefaultTags map[string]string
}

type RegexParserConfig struct {
	Pattern     string
	MetricGroup string
	ValueGroup  string
	TagGroups   []string
	DefaultTags map[string]string
}

func NewRegexParser(cfg RegexParserConfig) (*RegexParser, error) {
	re, err := regexp.Compile(cfg.Pattern)
	if err != nil {
		return nil, fmt.Errorf("regex compile: %w", err)
	}
	return &RegexParser{
		Pattern:     re,
		MetricGroup: cfg.MetricGroup,
		ValueGroup:  cfg.ValueGroup,
		TagGroups:   cfg.TagGroups,
		DefaultTags: cfg.DefaultTags,
	}, nil
}

func (p *RegexParser) Parse(line string) (*core.Sample, error) {
	matches := p.Pattern.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("no match")
	}
	s := &core.Sample{
		Tags: make(map[string]string),
	}
	for k, v := range p.DefaultTags {
		s.Tags[k] = v
	}
	for i, name := range p.Pattern.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		val := matches[i]
		switch name {
		case p.MetricGroup:
			s.Metric = val
		case p.ValueGroup:
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("parse value %q: %w", val, err)
			}
			s.Value = v
		default:
			for _, tag := range p.TagGroups {
				if name == tag {
					s.Tags[name] = val
				}
			}
		}
	}
	if s.Metric == "" {
		return nil, fmt.Errorf("metric group %q not found in match", p.MetricGroup)
	}
	s.Timestamp = time.Now().UnixMilli()
	return s, nil
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case json.Number:
		return val.Float64()
	case string:
		return strconv.ParseFloat(val, 64)
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case json.Number:
		return val.Int64()
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return 0, nil
		}
		return strconv.ParseInt(s, 10, 64)
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}
