#!/usr/bin/env bash
set -euo pipefail

BASE="${1:-http://localhost:8080}"
start_time="${2:-2026-06-21T00:00:00Z}"
end_time="${3:-2026-06-22T00:00:00Z}"

echo "=== Server health ==="
curl -sf "$BASE/healthz" && echo ""

echo ""
echo "=== List all metrics ==="
curl -s "$BASE/metrics" | python3 -m json.tool

echo ""
echo "=== Raw points (no aggregation) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.memory.used_percent\",\"start\":\"$start_time\",\"end\":\"$end_time\"}" \
  | python3 -m json.tool

echo ""
echo "=== Average (1h buckets) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.memory.used_percent\",\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"1h\",\"aggregation\":\"avg\"}" \
  | python3 -m json.tool

echo ""
echo "=== Min (30m buckets) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.cpu.usage\",\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"30m\",\"aggregation\":\"min\"}" \
  | python3 -m json.tool

echo ""
echo "=== Max (5m buckets) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.memory.used\",\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"5m\",\"aggregation\":\"max\"}" \
  | python3 -m json.tool

echo ""
echo "=== Sum (1h buckets) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.net.rx_bytes\",\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"1h\",\"aggregation\":\"sum\"}" \
  | python3 -m json.tool

echo ""
echo "=== Count (10m buckets) ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.memory.used_percent\",\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"10m\",\"aggregation\":\"count\"}" \
  | python3 -m json.tool

echo ""
echo "=== With tag filter ==="
curl -s -X POST "$BASE/query" \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"system.disk.read_bytes\",\"tags\":{\"device\":\"sda\"},\"start\":\"$start_time\",\"end\":\"$end_time\",\"bucket_width\":\"1h\",\"aggregation\":\"avg\"}" \
  | python3 -m json.tool

echo ""
echo "=== Series metadata ==="
curl -s "$BASE/series?metric=system.memory.used_percent" | python3 -m json.tool

echo ""
echo "=== Engine stats ==="
curl -s "$BASE/engine/metrics" | python3 -m json.tool
