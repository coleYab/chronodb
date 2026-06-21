package transform_test

import (
	"testing"

	"github.com/coleYab/chronodb/internal/transform"
)

func TestJSONParser_Basic(t *testing.T) {
	p := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField:    "endpoint",
		ValueField:     "duration_ms",
		TimestampField: "time",
		TagFields:      []string{"method", "status"},
	})

	line := `{"endpoint":"/api/login","method":"POST","status":200,"duration_ms":42.5,"time":1750500000000}`
	sample, err := p.Parse(line)
	if err != nil {
		t.Fatal(err)
	}

	if sample.Metric != "/api/login" {
		t.Fatalf("expected /api/login, got %s", sample.Metric)
	}
	if sample.Value != 42.5 {
		t.Fatalf("expected 42.5, got %f", sample.Value)
	}
	if sample.Timestamp != 1750500000000 {
		t.Fatalf("expected 1750500000000, got %d", sample.Timestamp)
	}
	if sample.Tags["method"] != "POST" {
		t.Fatalf("expected POST, got %s", sample.Tags["method"])
	}
	if sample.Tags["status"] != "200" {
		t.Fatalf("expected 200, got %s", sample.Tags["status"])
	}
}

func TestJSONParser_MissingMetricField(t *testing.T) {
	p := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "endpoint",
		ValueField:  "duration_ms",
	})
	_, err := p.Parse(`{"value": 1}`)
	if err == nil {
		t.Fatal("expected error for missing metric field")
	}
}

func TestJSONParser_MissingTimestampUsesDefault(t *testing.T) {
	p := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "value",
	})
	sample, err := p.Parse(`{"metric":"cpu","value":0.5}`)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Timestamp == 0 {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestJSONParser_DefaultTags(t *testing.T) {
	p := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "value",
		DefaultTags: map[string]string{"env": "prod", "host": "web-01"},
	})
	sample, err := p.Parse(`{"metric":"cpu","value":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Tags["env"] != "prod" {
		t.Fatalf("expected prod, got %s", sample.Tags["env"])
	}
	if sample.Tags["host"] != "web-01" {
		t.Fatalf("expected web-01, got %s", sample.Tags["host"])
	}
}

func TestJSONParser_ValueAsInt(t *testing.T) {
	p := transform.NewJSONParser(transform.JSONParserConfig{
		MetricField: "metric",
		ValueField:  "count",
	})
	sample, err := p.Parse(`{"metric":"requests","count":42}`)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Value != 42 {
		t.Fatalf("expected 42, got %f", sample.Value)
	}
}

func TestRegexParser_Basic(t *testing.T) {
	p, err := transform.NewRegexParser(transform.RegexParserConfig{
		Pattern:     `^(?P<metric>[\w.]+)\s+(?P<value>\d+\.\d+)\s+host=(?P<host>[\w-]+)$`,
		MetricGroup: "metric",
		ValueGroup:  "value",
		TagGroups:   []string{"host"},
	})
	if err != nil {
		t.Fatal(err)
	}

	sample, err := p.Parse("cpu.usage 42.5 host=web-01")
	if err != nil {
		t.Fatal(err)
	}

	if sample.Metric != "cpu.usage" {
		t.Fatalf("expected cpu.usage, got %s", sample.Metric)
	}
	if sample.Value != 42.5 {
		t.Fatalf("expected 42.5, got %f", sample.Value)
	}
	if sample.Tags["host"] != "web-01" {
		t.Fatalf("expected web-01, got %s", sample.Tags["host"])
	}
}

func TestRegexParser_NoMatch(t *testing.T) {
	p, err := transform.NewRegexParser(transform.RegexParserConfig{
		Pattern:     `^(?P<metric>\w+) (?P<value>\d+)$`,
		MetricGroup: "metric",
		ValueGroup:  "value",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Parse("invalid format here")
	if err == nil {
		t.Fatal("expected error for no match")
	}
}

func TestRegexParser_InvalidPattern(t *testing.T) {
	_, err := transform.NewRegexParser(transform.RegexParserConfig{
		Pattern: `[invalid`,
	})
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}
