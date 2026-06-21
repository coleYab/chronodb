package collector

import (
	"testing"
)

func TestStatsDParse_Gauge(t *testing.T) {
	tags := map[string]string{"host": "test"}
	s, err := parseStatsDLine("test.gauge:100|g", tags)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metric != "test.gauge" {
		t.Fatalf("expected test.gauge, got %s", s.Metric)
	}
	if s.Value != 100 {
		t.Fatalf("expected 100, got %f", s.Value)
	}
	if s.Tags["host"] != "test" {
		t.Fatalf("missing host tag")
	}
}

func TestStatsDParse_Counter(t *testing.T) {
	s, err := parseStatsDLine("requests:5|c", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metric != "requests" {
		t.Fatalf("expected requests, got %s", s.Metric)
	}
	if s.Value != 5 {
		t.Fatalf("expected 5, got %f", s.Value)
	}
}

func TestStatsDParse_CounterWithSampleRate(t *testing.T) {
	s, err := parseStatsDLine("requests:5|c|@0.5", nil)
	if err != nil {
		t.Fatal(err)
	}
	// With sample rate 0.5, value is adjusted: 5 / 0.5 = 10
	if s.Value != 10 {
		t.Fatalf("expected 10 (adjusted), got %f", s.Value)
	}
}

func TestStatsDParse_Timer(t *testing.T) {
	s, err := parseStatsDLine("response.time:150|ms", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metric != "response.time.ms" {
		t.Fatalf("expected response.time.ms, got %s", s.Metric)
	}
	if s.Value != 150 {
		t.Fatalf("expected 150, got %f", s.Value)
	}
}

func TestStatsDParse_Histogram(t *testing.T) {
	s, err := parseStatsDLine("latency:200|h", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metric != "latency.h" {
		t.Fatalf("expected latency.h, got %s", s.Metric)
	}
	if s.Value != 200 {
		t.Fatalf("expected 200, got %f", s.Value)
	}
}

func TestStatsDParse_Tags(t *testing.T) {
	s, err := parseStatsDLine("cpu:1|c|#env:prod,host:web01", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Tags["env"] != "prod" {
		t.Fatalf("expected env=prod, got %s", s.Tags["env"])
	}
	if s.Tags["host"] != "web01" {
		t.Fatalf("expected host=web01, got %s", s.Tags["host"])
	}
}

func TestStatsDParse_DefaultTagsMerged(t *testing.T) {
	s, err := parseStatsDLine("test:1|c|#env:prod", map[string]string{"host": "default"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Tags["env"] != "prod" {
		t.Fatalf("expected env=prod, got %s", s.Tags["env"])
	}
	if s.Tags["host"] != "default" {
		t.Fatalf("expected host=default, got %s", s.Tags["host"])
	}
}

func TestStatsDParse_Invalid(t *testing.T) {
	tests := []string{
		"",
		"nocolon",
		"nopipe:1",
		"badvalue:abc|c",
		"unknown:1|x",
	}
	for _, line := range tests {
		_, err := parseStatsDLine(line, nil)
		if err == nil {
			t.Fatalf("expected error for line %q", line)
		}
	}
}

func TestStatsDParse_TagOnlyKey(t *testing.T) {
	s, err := parseStatsDLine("test:1|c|#boolflag", nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Tags["boolflag"] != "true" {
		t.Fatalf("expected boolflag=true, got %s", s.Tags["boolflag"])
	}
}
