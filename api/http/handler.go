package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/memtable"
	"github.com/coleYab/chronodb/internal/query"
	"github.com/coleYab/chronodb/internal/series"
)

type Handler struct {
	engine   *engine.Engine
	registry *series.Registry
	index    *index.Index
}

func NewHandler(e *engine.Engine, reg *series.Registry, idx *index.Index) *Handler {
	return &Handler{engine: e, registry: reg, index: idx}
}

type writeRequest struct {
	Series []seriesWrite `json:"series"`
}

type seriesWrite struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Points []pointJSON       `json:"points"`
}

type pointJSON struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type writeResponse struct {
	Written int    `json:"written"`
	Error   string `json:"error,omitempty"`
}

func (h *Handler) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, writeResponse{Error: err.Error()})
		return
	}

	if len(req.Series) == 0 {
		writeJSON(w, http.StatusBadRequest, writeResponse{Error: "no series provided"})
		return
	}

	payloads := make(engine.BatchWritePayload, 0, len(req.Series))
	for _, sw := range req.Series {
		if len(sw.Points) == 0 {
			continue
		}
		seriesID, _ := h.registry.GetOrCreate(sw.Metric, sw.Tags)
		h.index.Insert(seriesID, sw.Metric, sw.Tags)

		pts := make([]memtable.Point, len(sw.Points))
		for i, p := range sw.Points {
			pts[i] = memtable.Point{Timestamp: p.Timestamp, Value: p.Value}
		}

		payloads = append(payloads, engine.WritePayload{SeriesID: seriesID, Points: pts})
	}

	cmd := engine.Command{
		Kind:    engine.BatchWriteCmd,
		Payload: payloads,
		RespCh:  make(chan engine.Response, 1),
	}
	if err := h.engine.Submit(cmd); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, writeResponse{Error: err.Error()})
		return
	}
	resp := <-cmd.RespCh
	if resp.Err != nil {
		writeJSON(w, http.StatusInternalServerError, writeResponse{Error: resp.Err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, writeResponse{Written: len(payloads)})
}

type queryRequest struct {
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags,omitempty"`
	Start       time.Time         `json:"start"`
	End         time.Time         `json:"end"`
	Aggregation string            `json:"aggregation,omitempty"`
	BucketWidth string            `json:"bucket_width,omitempty"`
}

type queryResponse struct {
	Results []queryResultJSON `json:"results"`
	Error   string            `json:"error,omitempty"`
}

type queryResultJSON struct {
	SeriesID uint64         `json:"series_id"`
	Buckets  []query.Bucket `json:"buckets,omitempty"`
	Points   []pointJSON    `json:"points,omitempty"`
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, queryResponse{Error: err.Error()})
		return
	}

	bucketWidth, err := time.ParseDuration(req.BucketWidth)
	if err != nil && req.BucketWidth != "" {
		writeJSON(w, http.StatusBadRequest, queryResponse{Error: "invalid bucket_width: " + err.Error()})
		return
	}

	payload := engine.QueryPayload{
		Metric:      req.Metric,
		TagFilters:  req.Tags,
		StartTime:   req.Start,
		EndTime:     req.End,
		Aggregation: req.Aggregation,
		BucketWidth: bucketWidth,
	}

	cmd := engine.Command{
		Kind:    engine.QueryCmd,
		Payload: payload,
		RespCh:  make(chan engine.Response, 1),
	}
	if err := h.engine.Submit(cmd); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, queryResponse{Error: err.Error()})
		return
	}

	resp := <-cmd.RespCh
	if resp.Err != nil {
		writeJSON(w, http.StatusInternalServerError, queryResponse{Error: resp.Err.Error()})
		return
	}

	results := resp.Data.([]query.Result)
	jsonResults := make([]queryResultJSON, len(results))
	for i, r := range results {
		jr := queryResultJSON{
			SeriesID: r.SeriesID,
			Buckets:  r.Buckets,
		}
		if len(r.Points) > 0 {
			jr.Points = make([]pointJSON, len(r.Points))
			for j, p := range r.Points {
				jr.Points[j] = pointJSON{Timestamp: p.Timestamp, Value: p.Value}
			}
		}
		jsonResults[i] = jr
	}

	writeJSON(w, http.StatusOK, queryResponse{Results: jsonResults})
}

func (h *Handler) handleListMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.index.ListMetrics()
	if len(metrics) == 0 {
		metrics = h.registry.ListMetrics()
	}
	writeJSON(w, http.StatusOK, metrics)
}

func (h *Handler) handleEngineMetrics(w http.ResponseWriter, r *http.Request) {
	m := h.engine.Metrics()
	writeJSON(w, http.StatusOK, map[string]int64{
		"points_written":    m.PointsWritten.Load(),
		"writes_ok":         m.WritesOK.Load(),
		"writes_error":      m.WritesError.Load(),
		"flushes_total":     m.FlushesTotal.Load(),
		"compactions_total": m.CompactionsTotal.Load(),
		"queries_total":     m.QueriesTotal.Load(),
	})
}

func (h *Handler) handleSeries(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "metric parameter required"})
		return
	}
	slist := h.registry.ListSeries(metric)
	if slist == nil {
		slist = []series.SeriesMeta{}
	}
	writeJSON(w, http.StatusOK, slist)
}

func (h *Handler) handleLanding(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, landingHTML)
}

func (h *Handler) handleDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, swaggerHTML)
}

func (h *Handler) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, openapiJSON)
}

func (h *Handler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !h.engine.Liveness() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/":
		h.handleLanding(w, r)
	case "/write":
		h.handleWrite(w, r)
	case "/query":
		h.handleQuery(w, r)
	case "/metrics":
		h.handleListMetrics(w, r)
	case "/engine/metrics":
		h.handleEngineMetrics(w, r)
	case "/series":
		h.handleSeries(w, r)
	case "/healthz":
		h.handleHealthz(w, r)
	case "/docs":
		h.handleDocs(w, r)
	case "/openapi.json":
		h.handleOpenAPI(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var landingHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chronodb</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #fff;
    color: #1a1a1a;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 2rem;
  }
  .container {
    max-width: 640px;
    width: 100%;
    text-align: center;
  }
  h1 {
    font-size: clamp(2.8rem, 12vw, 5rem);
    font-weight: 700;
    line-height: 1.1;
    margin-bottom: 1.25rem;
    color: #1a1a1a;
  }
  .desc {
    font-size: clamp(0.95rem, 4vw, 1.15rem);
    color: #444;
    line-height: 1.6;
    max-width: 540px;
    margin: 0 auto 2.5rem;
  }
  .buttons {
    display: flex;
    gap: 0.75rem;
    justify-content: center;
    flex-wrap: wrap;
  }
  .btn {
    display: inline-block;
    padding: 0.75rem 1.75rem;
    border-radius: 6px;
    font-size: 0.95rem;
    font-weight: 600;
    text-decoration: none;
    transition: opacity 0.2s;
  }
  .btn:hover { opacity: 0.85; }
  .btn-primary {
    background: #49cc90;
    color: #fff;
  }
  .btn-secondary {
    background: transparent;
    color: #49cc90;
    border: 2px solid #49cc90;
  }
  @media (max-width: 480px) {
    body { padding: 1.5rem; }
    .buttons { flex-direction: column; align-items: center; }
    .btn { width: 100%; max-width: 280px; text-align: center; }
  }
</style>
</head>
<body>
<div class="container">
  <h1>Chronodb</h1>
  <p class="desc">An ultra-fast, zero-dependency time series database engineered to store, aggregate, and query metrics effortlessly using standard HTTP requests.</p>
  <div class="buttons">
    <a href="/docs" class="btn btn-primary">Open Documentation</a>
    <a href="https://github.com/coleYab/tsdb" class="btn btn-secondary">GitHub Repository</a>
  </div>
</div>
</body>
</html>`

var swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ChronoDB API — Swagger</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
<style>
  body { margin: 0; background: #fff; }
  #swagger-ui { max-width: 1200px; margin: 0 auto; }
</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({
    url: '/openapi.json',
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [SwaggerUIBundle.presets.apis],
    layout: "BaseLayout",
  });
</script>
</body>
</html>`

type batchAddPoint struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
}

type batchAddRequest struct {
	Points []batchAddPoint `json:"points"`
}

type batchAddResponse struct {
	Written int    `json:"written"`
	Error   string `json:"error,omitempty"`
}

func (h *Handler) handleBatchAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req batchAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, batchAddResponse{Error: err.Error()})
		return
	}

	if len(req.Points) == 0 {
		writeJSON(w, http.StatusBadRequest, batchAddResponse{Error: "no points provided"})
		return
	}

	grouped := make(map[string]*engine.WritePayload)
	var keys []string

	for _, p := range req.Points {
		seriesID, _ := h.registry.GetOrCreate(p.Metric, p.Tags)
		h.index.Insert(seriesID, p.Metric, p.Tags)

		key := fmt.Sprintf("%s|%d", p.Metric, seriesID)
		wp, ok := grouped[key]
		if !ok {
			wp = &engine.WritePayload{SeriesID: seriesID}
			grouped[key] = wp
			keys = append(keys, key)
		}
		wp.Points = append(wp.Points, memtable.Point{Timestamp: p.Timestamp, Value: p.Value})
	}

	payloads := make(engine.BatchWritePayload, 0, len(keys))
	for _, k := range keys {
		payloads = append(payloads, *grouped[k])
	}

	cmd := engine.Command{
		Kind:    engine.BatchWriteCmd,
		Payload: payloads,
		RespCh:  make(chan engine.Response, 1),
	}
	if err := h.engine.Submit(cmd); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, batchAddResponse{Error: err.Error()})
		return
	}
	resp := <-cmd.RespCh
	if resp.Err != nil {
		writeJSON(w, http.StatusInternalServerError, batchAddResponse{Error: resp.Err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, batchAddResponse{Written: len(req.Points)})
}

var openapiJSON = `{
  "openapi": "3.1.0",
  "info": {
    "title": "ChronoDB API",
    "version": "1.0.0",
    "description": "Single-threaded, embedded time-series database HTTP API"
  },
  "servers": [
    { "url": "http://localhost:8080", "description": "Local development" }
  ],
  "paths": {
    "/write": {
      "post": {
        "summary": "Write data points",
        "description": "Write points to one or more time series.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/WriteRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Points written successfully",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/WriteResponse"
                }
              }
            }
          },
          "400": { "description": "Bad request" },
          "503": { "description": "Service unavailable" }
        }
      }
    },
    "/query": {
      "post": {
        "summary": "Query time-series data",
        "description": "Query time-series data with optional aggregation, bucketing, and tag filtering.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/QueryRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Query results",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/QueryResponse"
                }
              }
            }
          },
          "400": { "description": "Bad request" },
          "503": { "description": "Service unavailable" }
        }
      }
    },
    "/metrics": {
      "get": {
        "summary": "List metric names",
        "description": "List all metric names stored in the database.",
        "responses": {
          "200": {
            "description": "Array of metric name strings",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": { "type": "string" }
                }
              }
            }
          }
        }
      }
    },
    "/series": {
      "get": {
        "summary": "List series for a metric",
        "description": "List all time series (with tags) for a given metric name.",
        "parameters": [
          {
            "name": "metric",
            "in": "query",
            "required": true,
            "schema": { "type": "string" },
            "description": "Metric name to list series for"
          }
        ],
        "responses": {
          "200": {
            "description": "Array of series metadata",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "$ref": "#/components/schemas/SeriesMeta"
                  }
                }
              }
            }
          },
          "400": { "description": "Missing metric parameter" }
        }
      }
    },
    "/engine/metrics": {
      "get": {
        "summary": "Engine metrics",
        "description": "Internal engine counters including points written, flushes, compactions, and queries.",
        "responses": {
          "200": {
            "description": "Engine metrics",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/EngineMetrics"
                }
              }
            }
          }
        }
      }
    },
    "/healthz": {
      "get": {
        "summary": "Liveness probe",
        "description": "Returns 200 OK if the engine is healthy, or 503 if unhealthy.",
        "responses": {
          "200": {
            "description": "Healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string", "example": "ok" }
                  }
                }
              }
            }
          },
          "503": { "description": "Unhealthy" }
        }
      }
    },
    "/batch": {
      "post": {
        "summary": "Batch add data points",
        "description": "Add individual data points in bulk. Points are grouped by series automatically.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/BatchAddRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Points written successfully",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/BatchAddResponse"
                }
              }
            }
          },
          "400": { "description": "Bad request" },
          "503": { "description": "Service unavailable" }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "Point": {
        "type": "object",
        "properties": {
          "timestamp": { "type": "integer", "description": "Unix timestamp in milliseconds" },
          "value": { "type": "number", "format": "double" }
        }
      },
      "SeriesWrite": {
        "type": "object",
        "properties": {
          "metric": { "type": "string", "description": "Metric name" },
          "tags": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Key-value tag pairs" },
          "points": { "type": "array", "items": { "$ref": "#/components/schemas/Point" } }
        }
      },
      "WriteRequest": {
        "type": "object",
        "properties": {
          "series": { "type": "array", "items": { "$ref": "#/components/schemas/SeriesWrite" } }
        }
      },
      "WriteResponse": {
        "type": "object",
        "properties": {
          "written": { "type": "integer", "description": "Number of series written" },
          "error": { "type": "string" }
        }
      },
      "BatchAddPoint": {
        "type": "object",
        "properties": {
          "metric": { "type": "string", "description": "Metric name" },
          "tags": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Key-value tag pairs" },
          "timestamp": { "type": "integer", "description": "Unix timestamp in milliseconds" },
          "value": { "type": "number", "format": "double" }
        }
      },
      "BatchAddRequest": {
        "type": "object",
        "properties": {
          "points": { "type": "array", "items": { "$ref": "#/components/schemas/BatchAddPoint" } }
        }
      },
      "BatchAddResponse": {
        "type": "object",
        "properties": {
          "written": { "type": "integer", "description": "Number of points written" },
          "error": { "type": "string" }
        }
      },
      "QueryRequest": {
        "type": "object",
        "properties": {
          "metric": { "type": "string" },
          "tags": { "type": "object", "additionalProperties": { "type": "string" } },
          "start": { "type": "string", "format": "date-time", "description": "Start time in RFC3339 format" },
          "end": { "type": "string", "format": "date-time", "description": "End time in RFC3339 format" },
          "aggregation": { "type": "string", "enum": ["avg", "sum", "min", "max", "count"], "description": "Aggregation function" },
          "bucket_width": { "type": "string", "description": "Bucket width e.g. 1h, 5m" }
        }
      },
      "Bucket": {
        "type": "object",
        "properties": {
          "Start": { "type": "integer", "description": "Bucket start timestamp in ms" },
          "Count": { "type": "integer" },
          "Sum": { "type": "number" },
          "Min": { "type": "number" },
          "Max": { "type": "number" }
        }
      },
      "QueryResult": {
        "type": "object",
        "properties": {
          "series_id": { "type": "integer" },
          "buckets": { "type": "array", "items": { "$ref": "#/components/schemas/Bucket" } },
          "points": { "type": "array", "items": { "$ref": "#/components/schemas/Point" } }
        }
      },
      "QueryResponse": {
        "type": "object",
        "properties": {
          "results": { "type": "array", "items": { "$ref": "#/components/schemas/QueryResult" } },
          "error": { "type": "string" }
        }
      },
      "SeriesMeta": {
        "type": "object",
        "properties": {
          "series_id": { "type": "integer" },
          "metric": { "type": "string" },
          "tags": { "type": "object", "additionalProperties": { "type": "string" } }
        }
      },
      "EngineMetrics": {
        "type": "object",
        "properties": {
          "points_written": { "type": "integer" },
          "writes_ok": { "type": "integer" },
          "writes_error": { "type": "integer" },
          "flushes_total": { "type": "integer" },
          "compactions_total": { "type": "integer" },
          "queries_total": { "type": "integer" }
        }
      }
    }
  }
}`
