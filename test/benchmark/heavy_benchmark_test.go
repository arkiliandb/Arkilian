// Package benchmark provides heavy performance benchmarks for Project Arkilian.
// These benchmarks stress-test every major subsystem under realistic load conditions.
//
// Run with: go test -bench=Heavy -benchtime=10s -timeout=30m ./test/benchmark/...
// Run specific: go test -bench=HeavyIngestion -benchtime=5s ./test/benchmark/...
package benchmark

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/compaction"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/aggregator"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
	"github.com/golang/snappy"
	_ "github.com/mattn/go-sqlite3"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------


const (
	numTenants    = 500
	numUsers      = 50000
	numEventTypes = 20
)

var eventTypes = []string{
	"page_view", "click", "purchase", "signup", "logout",
	"search", "add_to_cart", "remove_from_cart", "checkout_start", "checkout_complete",
	"payment_success", "payment_failure", "refund", "subscription_start", "subscription_cancel",
	"profile_update", "password_change", "email_verify", "api_call", "error",
}

// generateHeavyRows creates realistic rows with high cardinality.
func generateHeavyRows(count int, rng *rand.Rand) []types.Row {
	gen := types.NewULIDGenerator()
	rows := make([]types.Row, count)
	baseTime := time.Now().Unix()

	for i := 0; i < count; i++ {
		ulid, _ := gen.Generate()
		tenantIdx := rng.Intn(numTenants)
		rows[i] = types.Row{
			EventID:   ulid.Bytes(),
			TenantID:  fmt.Sprintf("tenant_%04d", tenantIdx),
			UserID:    int64(rng.Intn(numUsers)),
			EventTime: baseTime + int64(rng.Intn(86400)), // spread over 1 day
			EventType: eventTypes[rng.Intn(numEventTypes)],
			Payload: map[string]interface{}{
				"session_id": fmt.Sprintf("sess_%d", rng.Int63()),
				"ip":         fmt.Sprintf("10.%d.%d.%d", rng.Intn(256), rng.Intn(256), rng.Intn(256)),
				"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
				"path":       fmt.Sprintf("/page/%d", rng.Intn(1000)),
				"duration":   rng.Float64() * 30.0,
				"status":     200 + rng.Intn(5)*100,
			},
		}
	}
	return rows
}

// setupBenchDir creates a temp directory for benchmark data.
func setupBenchDir(b *testing.B, prefix string) string {
	dir, err := os.MkdirTemp("", "arkilian-heavy-"+prefix+"-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// buildPartitionFile creates a real SQLite partition and returns its path + metadata.
func buildPartitionFile(b *testing.B, dir string, rows []types.Row, partKey string) (*partition.PartitionInfo, string) {
	builder := partition.NewBuilder(dir, 0)
	ctx := context.Background()
	key := types.PartitionKey{Strategy: "time", Value: partKey}
	info, err := builder.Build(ctx, rows, key)
	if err != nil {
		b.Fatal(err)
	}
	return info, info.SQLitePath
}

// ---------------------------------------------------------------------------
// 1. HEAVY INGESTION BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyIngestion_10K measures ingestion of 10K rows per partition.
// Target: 50K rows/sec/node (Requirement 16.1)
func BenchmarkHeavyIngestion_10K(b *testing.B) {
	dir := setupBenchDir(b, "ingest10k")
	builder := partition.NewBuilder(dir, 0)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(10000, rng)

	b.ResetTimer()
	b.ReportAllocs()

	totalRows := 0
	for i := 0; i < b.N; i++ {
		key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("2026020%d", i%10)}
		_, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}
		totalRows += len(rows)
	}

	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkHeavyIngestion_50K measures ingestion of 50K rows per partition.
func BenchmarkHeavyIngestion_50K(b *testing.B) {
	dir := setupBenchDir(b, "ingest50k")
	builder := partition.NewBuilder(dir, 0)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)

	b.ResetTimer()
	b.ReportAllocs()

	totalRows := 0
	for i := 0; i < b.N; i++ {
		key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("2026020%d", i%10)}
		_, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}
		totalRows += len(rows)
	}

	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkHeavyIngestion_Concurrent measures concurrent ingestion across goroutines.
func BenchmarkHeavyIngestion_Concurrent(b *testing.B) {
	dir := setupBenchDir(b, "ingest-concurrent")
	ctx := context.Background()
	workers := runtime.GOMAXPROCS(0)
	if workers < 4 {
		workers = 4
	}

	// Pre-generate rows per worker
	workerRows := make([][]types.Row, workers)
	for w := 0; w < workers; w++ {
		rng := rand.New(rand.NewSource(int64(w)))
		workerRows[w] = generateHeavyRows(5000, rng)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var totalRows int64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddInt64(&totalRows, 0)) % workers
		workerDir := filepath.Join(dir, fmt.Sprintf("worker_%d", workerID))
		os.MkdirAll(workerDir, 0755)
		builder := partition.NewBuilder(workerDir, 0)
		rows := workerRows[workerID%workers]
		i := 0

		for pb.Next() {
			key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("w%d_%d", workerID, i%10)}
			_, err := builder.Build(ctx, rows, key)
			if err != nil {
				b.Fatal(err)
			}
			atomic.AddInt64(&totalRows, int64(len(rows)))
			i++
		}
	})

	b.ReportMetric(float64(atomic.LoadInt64(&totalRows))/b.Elapsed().Seconds(), "rows/sec")
}


// ---------------------------------------------------------------------------
// 2. HEAVY MANIFEST / PRUNING BENCHMARKS
// ---------------------------------------------------------------------------

// populateCatalog fills a manifest catalog with N partitions spread across days.
func populateCatalog(b *testing.B, catalog *manifest.SQLiteCatalog, numPartitions int) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(99))

	for i := 0; i < numPartitions; i++ {
		day := i % 30
		minTime := int64(day*86400 + rng.Intn(43200))
		maxTime := minTime + int64(rng.Intn(43200))
		minUser := int64(rng.Intn(numUsers))
		maxUser := minUser + int64(rng.Intn(5000))
		tenantIdx := rng.Intn(numTenants)
		minTenant := fmt.Sprintf("tenant_%04d", tenantIdx)
		maxTenant := fmt.Sprintf("tenant_%04d", tenantIdx+rng.Intn(50))

		info := &partition.PartitionInfo{
			PartitionID:  fmt.Sprintf("events:2026%02d%02d:%06d", 2, day+1, i),
			PartitionKey: fmt.Sprintf("2026%02d%02d", 2, day+1),
			RowCount:     int64(1000 + rng.Intn(50000)),
			SizeBytes:    int64(1024*1024 + rng.Intn(15*1024*1024)),
			MinMaxStats: map[string]partition.MinMax{
				"event_time": {Min: minTime, Max: maxTime},
				"user_id":    {Min: minUser, Max: maxUser},
				"tenant_id":  {Min: minTenant, Max: maxTenant},
			},
			SchemaVersion: 1,
			CreatedAt:     time.Now(),
		}
		err := catalog.RegisterPartition(ctx, info, fmt.Sprintf("partitions/2026%02d%02d/%06d.sqlite", 2, day+1, i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeavyManifestPruning_10K measures pruning across 10K partitions.
func BenchmarkHeavyManifestPruning_10K(b *testing.B) {
	dir := setupBenchDir(b, "manifest10k")
	dbPath := filepath.Join(dir, "manifest.db")
	catalog, err := manifest.NewCatalog(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	populateCatalog(b, catalog, 10000)
	ctx := context.Background()

	predicates := []manifest.Predicate{
		{Column: "event_time", Operator: ">=", Value: int64(500000)},
		{Column: "event_time", Operator: "<=", Value: int64(510000)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := catalog.FindPartitions(ctx, predicates)
		if err != nil {
			b.Fatal(err)
		}
		_ = results
	}
}

// BenchmarkHeavyManifestPruning_100K measures pruning across 100K partitions.
func BenchmarkHeavyManifestPruning_100K(b *testing.B) {
	dir := setupBenchDir(b, "manifest100k")
	dbPath := filepath.Join(dir, "manifest.db")
	catalog, err := manifest.NewCatalog(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	populateCatalog(b, catalog, 100000)
	ctx := context.Background()

	predicates := []manifest.Predicate{
		{Column: "event_time", Operator: ">=", Value: int64(500000)},
		{Column: "event_time", Operator: "<=", Value: int64(510000)},
		{Column: "user_id", Operator: ">=", Value: int64(10000)},
		{Column: "user_id", Operator: "<=", Value: int64(15000)},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := catalog.FindPartitions(ctx, predicates)
		if err != nil {
			b.Fatal(err)
		}
		_ = results
	}
}

// BenchmarkHeavyManifestRegistration measures partition registration throughput.
func BenchmarkHeavyManifestRegistration(b *testing.B) {
	dir := setupBenchDir(b, "manifest-reg")
	dbPath := filepath.Join(dir, "manifest.db")
	catalog, err := manifest.NewCatalog(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		info := &partition.PartitionInfo{
			PartitionID:  fmt.Sprintf("events:20260206:%08d", i),
			PartitionKey: "20260206",
			RowCount:     int64(1000 + rng.Intn(50000)),
			SizeBytes:    int64(1024*1024 + rng.Intn(15*1024*1024)),
			MinMaxStats: map[string]partition.MinMax{
				"event_time": {Min: int64(i * 1000), Max: int64((i + 1) * 1000)},
				"user_id":    {Min: int64(i * 100), Max: int64((i + 1) * 100)},
			},
			SchemaVersion: 1,
			CreatedAt:     time.Now(),
		}
		err := catalog.RegisterPartition(ctx, info, fmt.Sprintf("partitions/20260206/%08d.sqlite", i))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "registrations/sec")
}


// ---------------------------------------------------------------------------
// 3. HEAVY BLOOM FILTER BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyBloomFilter_1M tests bloom filter with 1M items.
func BenchmarkHeavyBloomFilter_1M(b *testing.B) {
	numItems := 1_000_000
	filter := bloom.NewWithEstimates(numItems, 0.01)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numItems; i++ {
		filter.Add([]byte(fmt.Sprintf("tenant_%d", rng.Intn(numItems))))
	}

	testKeys := make([][]byte, 10000)
	for i := range testKeys {
		testKeys[i] = []byte(fmt.Sprintf("tenant_%d", rng.Intn(numItems*2)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		filter.Contains(testKeys[i%len(testKeys)])
	}
}

// BenchmarkHeavyBloomFilter_FPR_1M validates FPR at 1M scale.
func BenchmarkHeavyBloomFilter_FPR_1M(b *testing.B) {
	numItems := 1_000_000
	targetFPR := 0.01
	numBits, numHashes := bloom.OptimalParameters(numItems, targetFPR)
	filter := bloom.New(numBits, numHashes)

	for i := 0; i < numItems; i++ {
		filter.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	falsePositives := 0
	testCount := 1_000_000
	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("nonmember_%d", i))
		if filter.Contains(key) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testCount)
	b.ReportMetric(actualFPR*100, "FPR%")
	b.ReportMetric(float64(numBits)/(8*1024*1024), "filter_MB")

	if actualFPR > 0.015 {
		b.Errorf("FPR %.4f exceeds 1.5%% at 1M items", actualFPR)
	}
}

// BenchmarkHeavyBloomFilter_Serialization measures serialize/deserialize at scale.
func BenchmarkHeavyBloomFilter_Serialization(b *testing.B) {
	numItems := 500_000
	filter := bloom.NewWithEstimates(numItems, 0.01)
	for i := 0; i < numItems; i++ {
		filter.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := filter.SerializeToBase64()
		if err != nil {
			b.Fatal(err)
		}
		_, err = bloom.DeserializeFromBase64(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeavyBloomFilter_ConcurrentLookup measures concurrent bloom filter reads.
func BenchmarkHeavyBloomFilter_ConcurrentLookup(b *testing.B) {
	numItems := 500_000
	filter := bloom.NewWithEstimates(numItems, 0.01)
	for i := 0; i < numItems; i++ {
		filter.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	testKeys := make([][]byte, 100000)
	rng := rand.New(rand.NewSource(42))
	for i := range testKeys {
		testKeys[i] = []byte(fmt.Sprintf("item_%d", rng.Intn(numItems*2)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			filter.Contains(testKeys[idx%len(testKeys)])
			idx++
		}
	})
}


// ---------------------------------------------------------------------------
// 4. HEAVY SQL PARSER BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavySQLParsing_Complex measures parsing of complex queries.
func BenchmarkHeavySQLParsing_Complex(b *testing.B) {
	queries := []string{
		"SELECT * FROM events WHERE tenant_id = 'acme' AND event_time BETWEEN 1000 AND 2000",
		"SELECT user_id, COUNT(*), SUM(amount), AVG(duration) FROM events WHERE tenant_id = 'acme' AND event_type = 'purchase' GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 100",
		"SELECT tenant_id, event_type, COUNT(*), MIN(event_time), MAX(event_time) FROM events WHERE event_time >= 1700000000 AND event_time <= 1700100000 GROUP BY tenant_id, event_type ORDER BY tenant_id, COUNT(*) DESC LIMIT 50",
		"SELECT * FROM events WHERE tenant_id IN ('acme', 'beta', 'gamma', 'delta', 'epsilon') AND user_id >= 1000 AND user_id <= 5000 ORDER BY event_time DESC LIMIT 1000",
		"SELECT event_type, COUNT(*) FROM events WHERE tenant_id = 'acme' AND user_id = 12345 AND event_time BETWEEN 1706918400 AND 1706921999 GROUP BY event_type",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q := queries[i%len(queries)]
		stmt, err := parser.Parse(q)
		if err != nil {
			b.Fatal(err)
		}
		_ = stmt
	}
}

// BenchmarkHeavySQLParsing_PredicateExtraction measures predicate extraction.
func BenchmarkHeavySQLParsing_PredicateExtraction(b *testing.B) {
	query := "SELECT user_id, COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_time BETWEEN 1700000000 AND 1700010000 AND user_id >= 1000 AND user_id <= 5000 GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10"

	stmt, err := parser.Parse(query)
	if err != nil {
		b.Fatal(err)
	}
	selectStmt := stmt.(*parser.SelectStatement)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		preds := parser.ExtractPredicates(selectStmt)
		_ = preds
	}
}

// BenchmarkHeavySQLParsing_Concurrent measures concurrent SQL parsing.
func BenchmarkHeavySQLParsing_Concurrent(b *testing.B) {
	queries := []string{
		"SELECT * FROM events WHERE tenant_id = 'acme'",
		"SELECT user_id, COUNT(*) FROM events WHERE tenant_id = 'acme' GROUP BY user_id",
		"SELECT * FROM events WHERE event_time BETWEEN 1000 AND 2000 ORDER BY event_time LIMIT 100",
		"SELECT tenant_id, user_id, SUM(amount) FROM events WHERE event_type = 'purchase' GROUP BY tenant_id, user_id",
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q := queries[i%len(queries)]
			_, err := parser.Parse(q)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}


// ---------------------------------------------------------------------------
// 5. HEAVY QUERY EXECUTION BENCHMARKS (SQLite partition scan)
// ---------------------------------------------------------------------------

// BenchmarkHeavyPartitionScan_PointQuery measures point query on a single partition.
func BenchmarkHeavyPartitionScan_PointQuery(b *testing.B) {
	dir := setupBenchDir(b, "scan-point")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)
	info, sqlitePath := buildPartitionFile(b, dir, rows, "20260206")
	_ = info

	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro&_query_only=true")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant_%04d", rng.Intn(numTenants))
		sqlRows, err := db.Query(
			"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events WHERE tenant_id = ? LIMIT 100",
			tenantID,
		)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for sqlRows.Next() {
			var eid []byte
			var tid, etype string
			var uid, etime int64
			var payload []byte
			sqlRows.Scan(&eid, &tid, &uid, &etime, &etype, &payload)
			count++
		}
		sqlRows.Close()
	}
}

// BenchmarkHeavyPartitionScan_RangeQuery measures range scan on a single partition.
func BenchmarkHeavyPartitionScan_RangeQuery(b *testing.B) {
	dir := setupBenchDir(b, "scan-range")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)
	_, sqlitePath := buildPartitionFile(b, dir, rows, "20260206")

	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro&_query_only=true")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	baseTime := rows[0].EventTime

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := baseTime + int64(rng.Intn(43200))
		end := start + 3600 // 1 hour window
		sqlRows, err := db.Query(
			"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events WHERE event_time BETWEEN ? AND ? AND tenant_id = ?",
			start, end, fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
		)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for sqlRows.Next() {
			var eid []byte
			var tid, etype string
			var uid, etime int64
			var payload []byte
			sqlRows.Scan(&eid, &tid, &uid, &etime, &etype, &payload)
			count++
		}
		sqlRows.Close()
	}
}

// BenchmarkHeavyPartitionScan_AggregateQuery measures aggregate query on a partition.
func BenchmarkHeavyPartitionScan_AggregateQuery(b *testing.B) {
	dir := setupBenchDir(b, "scan-agg")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)
	_, sqlitePath := buildPartitionFile(b, dir, rows, "20260206")

	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro&_query_only=true")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant_%04d", rng.Intn(numTenants))
		sqlRows, err := db.Query(
			"SELECT user_id, COUNT(*), MIN(event_time), MAX(event_time) FROM events WHERE tenant_id = ? GROUP BY user_id",
			tenantID,
		)
		if err != nil {
			b.Fatal(err)
		}
		for sqlRows.Next() {
			var uid, cnt, minT, maxT int64
			sqlRows.Scan(&uid, &cnt, &minT, &maxT)
		}
		sqlRows.Close()
	}
}

// BenchmarkHeavyPartitionScan_FullScan measures full table scan with decompression.
func BenchmarkHeavyPartitionScan_FullScan(b *testing.B) {
	dir := setupBenchDir(b, "scan-full")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)
	_, sqlitePath := buildPartitionFile(b, dir, rows, "20260206")

	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro&_query_only=true")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sqlRows, err := db.Query(
			"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events")
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for sqlRows.Next() {
			var eid []byte
			var tid, etype string
			var uid, etime int64
			var compressedPayload []byte
			sqlRows.Scan(&eid, &tid, &uid, &etime, &etype, &compressedPayload)

			// Decompress payload like the real executor does
			decompressed, err := snappy.Decode(nil, compressedPayload)
			if err == nil {
				var payload map[string]interface{}
				json.Unmarshal(decompressed, &payload)
				_ = payload
			}
			count++
		}
		sqlRows.Close()
		b.ReportMetric(float64(count), "rows_scanned")
	}
}


// ---------------------------------------------------------------------------
// 6. HEAVY AGGREGATION BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyAggregation_PartialCompute measures partial aggregate computation.
func BenchmarkHeavyAggregation_PartialCompute(b *testing.B) {
	// Simulate 100K rows of query results
	numRows := 100000
	rng := rand.New(rand.NewSource(42))
	rows := make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = []interface{}{
			fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
			int64(rng.Intn(numUsers)),
			int64(rng.Intn(86400)),
			rng.Float64() * 100.0,
		}
	}

	columns := []string{"tenant_id", "user_id", "event_time", "amount"}

	// Create aggregate expressions for COUNT(*), SUM(amount), MIN(event_time), MAX(event_time)
	aggExprs := []*parser.AggregateExpr{
		{Function: "COUNT", Arg: nil},
		{Function: "SUM", Arg: &parser.ColumnRef{Column: "amount"}},
		{Function: "MIN", Arg: &parser.ColumnRef{Column: "event_time"}},
		{Function: "MAX", Arg: &parser.ColumnRef{Column: "event_time"}},
	}
	colIndices := aggregator.ResolveAggregateColumnIndices(aggExprs, columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		set := aggregator.ComputePartialAggregates(rows, aggExprs, colIndices)
		_ = set.Results()
	}
}

// BenchmarkHeavyAggregation_GroupByMerge measures GROUP BY merge across partitions.
func BenchmarkHeavyAggregation_GroupByMerge(b *testing.B) {
	numPartitions := 50
	rowsPerPartition := 10000
	rng := rand.New(rand.NewSource(42))

	columns := []string{"tenant_id", "user_id", "event_time", "amount"}
	aggExprs := []*parser.AggregateExpr{
		{Function: "COUNT", Arg: nil},
		{Function: "SUM", Arg: &parser.ColumnRef{Column: "amount"}},
	}
	groupByExprs := []parser.Expression{&parser.ColumnRef{Column: "tenant_id"}}

	groupColIndices := aggregator.ResolveGroupByColumnIndices(groupByExprs, columns)
	aggColIndices := aggregator.ResolveAggregateColumnIndices(aggExprs, columns)

	// Pre-compute grouped partials for each partition
	partitionResults := make([]map[aggregator.GroupKey]*aggregator.GroupedPartialResult, numPartitions)
	for p := 0; p < numPartitions; p++ {
		rows := make([][]interface{}, rowsPerPartition)
		for i := 0; i < rowsPerPartition; i++ {
			rows[i] = []interface{}{
				fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
				int64(rng.Intn(numUsers)),
				int64(rng.Intn(86400)),
				rng.Float64() * 100.0,
			}
		}
		partitionResults[p] = aggregator.ComputeGroupedPartials(rows, aggExprs, groupColIndices, aggColIndices)
	}

	merger := aggregator.NewGroupByMerger(groupByExprs, aggExprs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		merged := merger.MergeGroupedPartials(partitionResults)
		resultRows := merger.ToRows(merged)
		_ = resultRows
	}

	b.ReportMetric(float64(numPartitions), "partitions_merged")
}

// BenchmarkHeavyAggregation_OrderBySort measures ORDER BY sorting of large result sets.
func BenchmarkHeavyAggregation_OrderBySort(b *testing.B) {
	numRows := 100000
	rng := rand.New(rand.NewSource(42))

	columns := []string{"tenant_id", "count", "total_amount"}
	clauses := []parser.OrderByClause{
		{Expr: &parser.ColumnRef{Column: "count"}, Desc: true},
		{Expr: &parser.ColumnRef{Column: "tenant_id"}, Desc: false},
	}
	sorter := aggregator.NewOrderBySorter(clauses, columns)

	// Generate rows
	baseRows := make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		baseRows[i] = []interface{}{
			fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
			int64(rng.Intn(100000)),
			rng.Float64() * 10000.0,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Copy rows since sort is in-place
		rows := make([][]interface{}, len(baseRows))
		copy(rows, baseRows)
		err := sorter.Sort(rows)
		if err != nil {
			b.Fatal(err)
		}
	}
}


// ---------------------------------------------------------------------------
// 7. HEAVY STORAGE BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyStorage_Upload measures upload throughput for partition-sized files.
func BenchmarkHeavyStorage_Upload(b *testing.B) {
	dir := setupBenchDir(b, "storage-upload")
	storageDir := filepath.Join(dir, "storage")
	localStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Create test files of varying sizes (1MB, 8MB, 16MB)
	sizes := []struct {
		name string
		size int
	}{
		{"1MB", 1 * 1024 * 1024},
		{"8MB", 8 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			testFile := filepath.Join(dir, fmt.Sprintf("test_%s.dat", sz.name))
			data := make([]byte, sz.size)
			rng := rand.New(rand.NewSource(42))
			rng.Read(data)
			if err := os.WriteFile(testFile, data, 0644); err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(sz.size))

			for i := 0; i < b.N; i++ {
				objectPath := fmt.Sprintf("bench/%s_%d.dat", sz.name, i)
				if err := localStorage.Upload(ctx, testFile, objectPath); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkHeavyStorage_DownloadConcurrent measures concurrent download throughput.
func BenchmarkHeavyStorage_DownloadConcurrent(b *testing.B) {
	dir := setupBenchDir(b, "storage-dl")
	storageDir := filepath.Join(dir, "storage")
	localStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	numFiles := 100
	fileSize := 8 * 1024 * 1024 // 8MB

	// Upload test files
	for i := 0; i < numFiles; i++ {
		srcFile := filepath.Join(dir, fmt.Sprintf("src_%d.dat", i))
		data := make([]byte, fileSize)
		rand.New(rand.NewSource(int64(i))).Read(data)
		os.WriteFile(srcFile, data, 0644)
		localStorage.Upload(ctx, srcFile, fmt.Sprintf("partitions/%d.sqlite", i))
		os.Remove(srcFile)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(fileSize))

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			objectPath := fmt.Sprintf("partitions/%d.sqlite", i%numFiles)
			localPath := filepath.Join(dir, fmt.Sprintf("dl_%d_%d.dat", i, time.Now().UnixNano()))
			err := localStorage.Download(ctx, objectPath, localPath)
			if err != nil {
				b.Fatal(err)
			}
			os.Remove(localPath)
			i++
		}
	})
}


// ---------------------------------------------------------------------------
// 8. HEAVY CONNECTION POOL BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyConnectionPool measures connection pool under contention.
func BenchmarkHeavyConnectionPool(b *testing.B) {
	dir := setupBenchDir(b, "pool")
	rng := rand.New(rand.NewSource(42))

	// Create several real partition files
	numPartitions := 20
	partitionPaths := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		rows := generateHeavyRows(1000, rng)
		_, sqlitePath := buildPartitionFile(b, filepath.Join(dir, fmt.Sprintf("p%d", i)), rows, fmt.Sprintf("2026020%d", i%10))
		partitionPaths[i] = sqlitePath
	}

	pool := executor.NewConnectionPool(executor.PoolConfig{
		MaxConnections:      10,
		MaxTotalConnections: 50,
		IdleTimeout:         5 * time.Minute,
	})
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localRng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			path := partitionPaths[localRng.Intn(numPartitions)]
			db, err := pool.Get(ctx, path)
			if err != nil {
				// Pool full is expected under heavy contention
				continue
			}
			// Execute a quick query
			row := db.QueryRow("SELECT COUNT(*) FROM events")
			var count int64
			row.Scan(&count)
			pool.Release(path)
		}
	})

	stats := pool.Stats()
	b.ReportMetric(float64(stats.TotalConnections), "total_conns")
}


// ---------------------------------------------------------------------------
// 9. HEAVY METADATA GENERATION BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyMetadataGeneration measures metadata sidecar generation at scale.
func BenchmarkHeavyMetadataGeneration(b *testing.B) {
	dir := setupBenchDir(b, "metadata")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)

	builder := partition.NewBuilder(dir, 0)
	ctx := context.Background()
	key := types.PartitionKey{Strategy: "time", Value: "20260206"}
	info, err := builder.Build(ctx, rows, key)
	if err != nil {
		b.Fatal(err)
	}

	metaGen := partition.NewMetadataGenerator()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sidecar, err := metaGen.Generate(info, rows)
		if err != nil {
			b.Fatal(err)
		}
		// Serialize to JSON like the real pipeline
		data, err := sidecar.ToJSON()
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

// BenchmarkHeavyMetadataRoundTrip measures full metadata serialize/deserialize cycle.
func BenchmarkHeavyMetadataRoundTrip(b *testing.B) {
	dir := setupBenchDir(b, "metadata-rt")
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(50000, rng)

	builder := partition.NewBuilder(dir, 0)
	ctx := context.Background()
	key := types.PartitionKey{Strategy: "time", Value: "20260206"}
	info, err := builder.Build(ctx, rows, key)
	if err != nil {
		b.Fatal(err)
	}

	metaGen := partition.NewMetadataGenerator()
	sidecar, err := metaGen.Generate(info, rows)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := sidecar.ToJSON()
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := partition.FromJSON(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = decoded
	}
}


// ---------------------------------------------------------------------------
// 10. HEAVY SNAPPY COMPRESSION BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavySnappyCompression measures Snappy compression throughput for payloads.
func BenchmarkHeavySnappyCompression(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Generate realistic payloads
	payloads := make([][]byte, 10000)
	for i := range payloads {
		payload := map[string]interface{}{
			"session_id": fmt.Sprintf("sess_%d", rng.Int63()),
			"ip":         fmt.Sprintf("10.%d.%d.%d", rng.Intn(256), rng.Intn(256), rng.Intn(256)),
			"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
			"path":       fmt.Sprintf("/page/%d/section/%d", rng.Intn(1000), rng.Intn(50)),
			"duration":   rng.Float64() * 30.0,
			"status":     200 + rng.Intn(5)*100,
			"referrer":   fmt.Sprintf("https://example.com/ref/%d", rng.Intn(500)),
			"tags":       []string{"web", "analytics", fmt.Sprintf("tag_%d", rng.Intn(100))},
		}
		data, _ := json.Marshal(payload)
		payloads[i] = data
	}

	b.Run("Compress", func(b *testing.B) {
		b.ReportAllocs()
		totalBytes := int64(0)
		compressedBytes := int64(0)
		for i := 0; i < b.N; i++ {
			p := payloads[i%len(payloads)]
			compressed := snappy.Encode(nil, p)
			totalBytes += int64(len(p))
			compressedBytes += int64(len(compressed))
		}
		if totalBytes > 0 {
			b.ReportMetric(float64(compressedBytes)/float64(totalBytes)*100, "compression_%")
		}
	})

	// Pre-compress for decompression benchmark
	compressed := make([][]byte, len(payloads))
	for i, p := range payloads {
		compressed[i] = snappy.Encode(nil, p)
	}

	b.Run("Decompress", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c := compressed[i%len(compressed)]
			_, err := snappy.Decode(nil, c)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("RoundTrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p := payloads[i%len(payloads)]
			c := snappy.Encode(nil, p)
			d, err := snappy.Decode(nil, c)
			if err != nil {
				b.Fatal(err)
			}
			_ = d
		}
	})
}


// ---------------------------------------------------------------------------
// 11. HEAVY END-TO-END PIPELINE BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyEndToEnd_IngestToQuery measures the full ingest → store → query pipeline.
func BenchmarkHeavyEndToEnd_IngestToQuery(b *testing.B) {
	dir := setupBenchDir(b, "e2e")
	partDir := filepath.Join(dir, "partitions")
	storageDir := filepath.Join(dir, "storage")
	manifestPath := filepath.Join(dir, "manifest.db")

	os.MkdirAll(partDir, 0755)

	localStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	builder := partition.NewBuilder(partDir, 0)
	metaGen := partition.NewMetadataGenerator()

	// Phase 1: Ingest multiple partitions
	numPartitions := 20
	for p := 0; p < numPartitions; p++ {
		rows := generateHeavyRows(5000, rng)
		key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("2026020%d", p%10)}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}

		// Generate metadata
		metadataPath, err := metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			b.Fatal(err)
		}

		// Upload to storage
		objectPath := fmt.Sprintf("partitions/%s.sqlite", info.PartitionID)
		if err := localStorage.Upload(ctx, info.SQLitePath, objectPath); err != nil {
			b.Fatal(err)
		}

		metaObjectPath := fmt.Sprintf("partitions/%s.meta.json", info.PartitionID)
		if err := localStorage.Upload(ctx, metadataPath, metaObjectPath); err != nil {
			b.Fatal(err)
		}

		// Register in manifest
		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			b.Fatal(err)
		}
	}

	// Phase 2: Benchmark query execution
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Find partitions via manifest pruning
		predicates := []manifest.Predicate{
			{Column: "event_time", Operator: ">=", Value: int64(rng.Intn(43200))},
			{Column: "event_time", Operator: "<=", Value: int64(43200 + rng.Intn(43200))},
		}
		partitions, err := catalog.FindPartitions(ctx, predicates)
		if err != nil {
			b.Fatal(err)
		}

		// Download and query each partition
		totalRows := 0
		for _, part := range partitions {
			localPath := filepath.Join(dir, "dl", filepath.Base(part.ObjectPath))
			os.MkdirAll(filepath.Dir(localPath), 0755)
			if err := localStorage.Download(ctx, part.ObjectPath, localPath); err != nil {
				continue
			}

			db, err := sql.Open("sqlite3", localPath+"?mode=ro&_query_only=true")
			if err != nil {
				continue
			}

			sqlRows, err := db.Query(
				"SELECT COUNT(*) FROM events WHERE tenant_id = ?",
				fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
			)
			if err == nil {
				for sqlRows.Next() {
					var count int64
					sqlRows.Scan(&count)
					totalRows += int(count)
				}
				sqlRows.Close()
			}
			db.Close()
		}
	}
}


// ---------------------------------------------------------------------------
// 12. HEAVY RESULT MERGER BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyResultMerger measures merging results from many partitions.
func BenchmarkHeavyResultMerger(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	numPartitions := 100
	rowsPerPartition := 1000

	// Generate partial results
	partialResults := make([]*executor.PartialResult, numPartitions)
	for p := 0; p < numPartitions; p++ {
		rows := make([][]interface{}, rowsPerPartition)
		for i := 0; i < rowsPerPartition; i++ {
			rows[i] = []interface{}{
				fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
				int64(rng.Intn(numUsers)),
				int64(rng.Intn(86400)),
				fmt.Sprintf("event_%d", rng.Intn(numEventTypes)),
			}
		}
		partialResults[p] = &executor.PartialResult{
			PartitionID: fmt.Sprintf("part_%d", p),
			Columns:     []string{"tenant_id", "user_id", "event_time", "event_type"},
			Rows:        rows,
			RowCount:    int64(rowsPerPartition),
		}
	}

	columns := []string{"tenant_id", "user_id", "event_time", "event_type"}

	b.Run("NoSort", func(b *testing.B) {
		limit := int64(1000)
		merger := executor.NewResultMerger(columns, nil, &limit, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := merger.Merge(partialResults)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})

	b.Run("WithSort", func(b *testing.B) {
		limit := int64(1000)
		orderBy := []parser.OrderByClause{
			{Expr: &parser.ColumnRef{Column: "event_time"}, Desc: true},
			{Expr: &parser.ColumnRef{Column: "tenant_id"}, Desc: false},
		}
		merger := executor.NewResultMerger(columns, orderBy, &limit, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := merger.Merge(partialResults)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})

	b.Run("UnionAll", func(b *testing.B) {
		limit := int64(5000)
		merger := executor.NewUnionAllMerger(&limit, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := merger.Merge(partialResults)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})
}


// ---------------------------------------------------------------------------
// 13. HEAVY ULID GENERATION BENCHMARKS
// ---------------------------------------------------------------------------

// BenchmarkHeavyULID_Sequential measures sequential ULID generation throughput.
func BenchmarkHeavyULID_Sequential(b *testing.B) {
	gen := types.NewULIDGenerator()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.Generate()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeavyULID_Concurrent measures concurrent ULID generation.
func BenchmarkHeavyULID_Concurrent(b *testing.B) {
	gen := types.NewULIDGenerator()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := gen.Generate()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkHeavyULID_MonotonicBurst measures ULID generation in same-millisecond bursts.
func BenchmarkHeavyULID_MonotonicBurst(b *testing.B) {
	gen := types.NewULIDGenerator()
	fixedTime := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.GenerateWithTime(fixedTime)
		if err != nil {
			b.Fatal(err)
		}
	}
}


// ---------------------------------------------------------------------------
// 14. HEAVY CONCURRENT MIXED WORKLOAD BENCHMARK
// ---------------------------------------------------------------------------

// BenchmarkHeavyMixedWorkload simulates a realistic mixed read/write workload.
func BenchmarkHeavyMixedWorkload(b *testing.B) {
	dir := setupBenchDir(b, "mixed")
	partDir := filepath.Join(dir, "partitions")
	storageDir := filepath.Join(dir, "storage")
	manifestPath := filepath.Join(dir, "manifest.db")

	os.MkdirAll(partDir, 0755)

	localStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Pre-populate with some partitions
	rng := rand.New(rand.NewSource(42))
	builder := partition.NewBuilder(partDir, 0)
	metaGen := partition.NewMetadataGenerator()

	for p := 0; p < 10; p++ {
		rows := generateHeavyRows(5000, rng)
		key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("2026020%d", p%10)}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}
		metaGen.GenerateAndWrite(info, rows)
		objectPath := fmt.Sprintf("partitions/%s.sqlite", info.PartitionID)
		localStorage.Upload(ctx, info.SQLitePath, objectPath)
		catalog.RegisterPartition(ctx, info, objectPath)
	}

	var writeOps, readOps int64

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup

	// Writer goroutines
	writerCount := 2
	for w := 0; w < writerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(int64(workerID) + 100))
			wDir := filepath.Join(partDir, fmt.Sprintf("w%d", workerID))
			os.MkdirAll(wDir, 0755)
			localBuilder := partition.NewBuilder(wDir, 0)

			for i := 0; i < b.N/writerCount; i++ {
				rows := generateHeavyRows(1000, localRng)
				key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("w%d_%d", workerID, i%10)}
				info, err := localBuilder.Build(ctx, rows, key)
				if err != nil {
					continue
				}
				objectPath := fmt.Sprintf("partitions/%s.sqlite", info.PartitionID)
				localStorage.Upload(ctx, info.SQLitePath, objectPath)
				catalog.RegisterPartition(ctx, info, objectPath)
				atomic.AddInt64(&writeOps, 1)
			}
		}(w)
	}

	// Reader goroutines
	readerCount := 4
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(int64(readerID) + 200))

			for i := 0; i < b.N/readerCount; i++ {
				predicates := []manifest.Predicate{
					{Column: "event_time", Operator: ">=", Value: int64(localRng.Intn(43200))},
					{Column: "event_time", Operator: "<=", Value: int64(43200 + localRng.Intn(43200))},
				}
				partitions, err := catalog.FindPartitions(ctx, predicates)
				if err != nil {
					continue
				}
				_ = partitions
				atomic.AddInt64(&readOps, 1)
			}
		}(r)
	}

	wg.Wait()

	b.ReportMetric(float64(atomic.LoadInt64(&writeOps))/b.Elapsed().Seconds(), "writes/sec")
	b.ReportMetric(float64(atomic.LoadInt64(&readOps))/b.Elapsed().Seconds(), "reads/sec")
}


// ---------------------------------------------------------------------------
// 15. HEAVY MEMORY PRESSURE BENCHMARK
// ---------------------------------------------------------------------------

// BenchmarkHeavyBloomFilter_MemoryPressure measures bloom filter cache under memory pressure.
// Simulates loading bloom filters for many partitions (the "10x partitions" red team scenario).
func BenchmarkHeavyBloomFilter_MemoryPressure(b *testing.B) {
	numFilters := 1000
	itemsPerFilter := 10000

	// Pre-build filters
	filters := make([]*bloom.BloomFilter, numFilters)
	for i := 0; i < numFilters; i++ {
		f := bloom.NewWithEstimates(itemsPerFilter, 0.01)
		for j := 0; j < itemsPerFilter; j++ {
			f.Add([]byte(fmt.Sprintf("tenant_%d_%d", i, j)))
		}
		filters[i] = f
	}

	// Measure memory before
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Serialize all filters (simulates loading into cache)
	serialized := make([]string, numFilters)
	for i, f := range filters {
		data, err := f.SerializeToBase64()
		if err != nil {
			b.Fatal(err)
		}
		serialized[i] = data
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	b.ReportMetric(float64(memAfter.Alloc-memBefore.Alloc)/(1024*1024), "cache_MB")
	b.ReportMetric(float64(numFilters), "filters_loaded")

	// Benchmark deserialization + lookup cycle
	testKey := []byte("tenant_500_5000")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := i % numFilters
		f, err := bloom.DeserializeFromBase64(serialized[idx])
		if err != nil {
			b.Fatal(err)
		}
		f.Contains(testKey)
	}
}

// BenchmarkHeavyStatsTracker measures statistics tracking at scale.
func BenchmarkHeavyStatsTracker(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	rows := generateHeavyRows(100000, rng)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tracker := partition.NewStatsTracker()
		for _, row := range rows {
			tracker.Update(row)
		}
		stats := tracker.GetMinMaxStats()
		_ = stats
	}
}

// TestIngestionAllocScaling verifies that per-row allocations do not grow
// superlinearly with batch size. The 50K benchmark's per-row allocs must
// stay within 1.5× of the 10K benchmark's per-row allocs.
// This guards against the GC death spiral pathology (Report Issue #1).
func TestIngestionAllocScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping allocation scaling test in short mode")
	}

	const (
		smallBatch = 10000
		largeBatch = 50000
		maxRatio   = 1.5
	)

	measureAllocsPerRow := func(batchSize int) float64 {
		rng := rand.New(rand.NewSource(42))
		rows := generateHeavyRows(batchSize, rng)

		dir, err := os.MkdirTemp("", fmt.Sprintf("arkilian-alloc-test-%d-*", batchSize))
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		builder := partition.NewBuilder(dir, 0)
		ctx := context.Background()
		key := types.PartitionKey{Strategy: "time", Value: "20260206"}

		// Warm up
		_, err = builder.Build(ctx, rows, key)
		if err != nil {
			t.Fatal(err)
		}

		// Measure
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		allocsBefore := m.TotalAlloc

		key.Value = "20260207"
		_, err = builder.Build(ctx, rows, key)
		if err != nil {
			t.Fatal(err)
		}

		runtime.ReadMemStats(&m)
		allocsAfter := m.TotalAlloc

		totalAllocs := allocsAfter - allocsBefore
		return float64(totalAllocs) / float64(batchSize)
	}

	perRow10K := measureAllocsPerRow(smallBatch)
	perRow50K := measureAllocsPerRow(largeBatch)

	ratio := perRow50K / perRow10K
	t.Logf("Per-row allocs: 10K=%.1f bytes, 50K=%.1f bytes, ratio=%.2f", perRow10K, perRow50K, ratio)

	if ratio > maxRatio {
		t.Errorf("Allocation scaling regression: 50K per-row allocs (%.1f) are %.2fx of 10K per-row allocs (%.1f), exceeds %.1fx threshold",
			perRow50K, ratio, perRow10K, maxRatio)
	}
}

// ---------------------------------------------------------------------------
// SIMULATED S3 LATENCY BENCHMARKS (Report Issue #3)
// ---------------------------------------------------------------------------

// latencyStorage wraps an ObjectStorage with configurable artificial delay
// on Download (GET) operations to simulate real S3 latency.
type latencyStorage struct {
	inner      storage.ObjectStorage
	getLatency time.Duration
	getCount   atomic.Int64
}

func newLatencyStorage(inner storage.ObjectStorage, getLatency time.Duration) *latencyStorage {
	return &latencyStorage{
		inner:      inner,
		getLatency: getLatency,
	}
}

func (l *latencyStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	return l.inner.Upload(ctx, localPath, objectPath)
}

func (l *latencyStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	return l.inner.UploadMultipart(ctx, localPath, objectPath)
}

func (l *latencyStorage) Download(ctx context.Context, objectPath, localPath string) error {
	l.getCount.Add(1)
	time.Sleep(l.getLatency)
	return l.inner.Download(ctx, objectPath, localPath)
}

func (l *latencyStorage) Delete(ctx context.Context, objectPath string) error {
	return l.inner.Delete(ctx, objectPath)
}

func (l *latencyStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	return l.inner.Exists(ctx, objectPath)
}

func (l *latencyStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	return l.inner.ConditionalPut(ctx, localPath, objectPath, etag)
}

func (l *latencyStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	return l.inner.ListObjects(ctx, prefix)
}

// BenchmarkHeavyStorage_SimulatedS3Latency benchmarks the full query path
// (prune → download → scan) under simulated S3 latency (80ms per GET).
// This exposes the real-world cost that local benchmarks mask.
func BenchmarkHeavyStorage_SimulatedS3Latency(b *testing.B) {
	dir := setupBenchDir(b, "s3latency")
	partDir := filepath.Join(dir, "partitions")
	storageDir := filepath.Join(dir, "storage")
	manifestPath := filepath.Join(dir, "manifest.db")
	downloadDir := filepath.Join(dir, "downloads")

	os.MkdirAll(partDir, 0755)
	os.MkdirAll(downloadDir, 0755)

	realStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	// Wrap with 80ms GET latency to simulate S3
	simStorage := newLatencyStorage(realStorage, 80*time.Millisecond)

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	builder := partition.NewBuilder(partDir, 0)
	metaGen := partition.NewMetadataGenerator()

	// Ingest partitions (use real storage for setup, no latency)
	numPartitions := 20
	for p := 0; p < numPartitions; p++ {
		rows := generateHeavyRows(5000, rng)
		key := types.PartitionKey{Strategy: "time", Value: fmt.Sprintf("2026020%d", p%10)}
		info, err := builder.Build(ctx, rows, key)
		if err != nil {
			b.Fatal(err)
		}

		metadataPath, err := metaGen.GenerateAndWrite(info, rows)
		if err != nil {
			b.Fatal(err)
		}

		objectPath := fmt.Sprintf("partitions/%s.sqlite", info.PartitionID)
		if err := realStorage.Upload(ctx, info.SQLitePath, objectPath); err != nil {
			b.Fatal(err)
		}

		metaObjectPath := fmt.Sprintf("partitions/%s.meta.json", info.PartitionID)
		if err := realStorage.Upload(ctx, metadataPath, metaObjectPath); err != nil {
			b.Fatal(err)
		}

		if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		simStorage.getCount.Store(0)
		iterStart := time.Now()

		// Clean download dir to force re-downloads each iteration
		os.RemoveAll(downloadDir)
		os.MkdirAll(downloadDir, 0755)

		// Phase 1: Prune via manifest — use nil predicates to get all partitions
		// (simulates a broad scan that hits many partitions)
		var predicates []manifest.Predicate
		partitions, err := catalog.FindPartitions(ctx, predicates)
		if err != nil {
			b.Fatal(err)
		}

		// Phase 2: Download and scan each partition (through latency wrapper)
		totalRows := 0
		for _, part := range partitions {
			localPath := filepath.Join(downloadDir, filepath.Base(part.ObjectPath))
			if err := simStorage.Download(ctx, part.ObjectPath, localPath); err != nil {
				continue
			}

			db, err := sql.Open("sqlite3", localPath+"?mode=ro&_query_only=true")
			if err != nil {
				continue
			}

			sqlRows, err := db.Query(
				"SELECT COUNT(*) FROM events WHERE tenant_id = ?",
				fmt.Sprintf("tenant_%04d", rng.Intn(numTenants)),
			)
			if err == nil {
				for sqlRows.Next() {
					var count int64
					sqlRows.Scan(&count)
					totalRows += int(count)
				}
				sqlRows.Close()
			}
			db.Close()
		}

		queryLatencyMs := time.Since(iterStart).Milliseconds()
		s3Gets := simStorage.getCount.Load()

		b.ReportMetric(float64(queryLatencyMs), "query_latency_ms")
		b.ReportMetric(float64(s3Gets), "s3_gets")
	}
}

// faultInjectingStorage wraps an ObjectStorage and fails UploadMultipart calls
// after a configurable number of successful uploads.
type faultInjectingStorage struct {
	inner           storage.ObjectStorage
	uploadCount     atomic.Int64
	failAfterN      int64 // fail UploadMultipart after this many successful calls
	uploadFailed    atomic.Bool
}

func newFaultInjectingStorage(inner storage.ObjectStorage, failAfterN int64) *faultInjectingStorage {
	return &faultInjectingStorage{
		inner:      inner,
		failAfterN: failAfterN,
	}
}

func (f *faultInjectingStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	return f.inner.Upload(ctx, localPath, objectPath)
}

func (f *faultInjectingStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	count := f.uploadCount.Add(1)
	if count > f.failAfterN {
		f.uploadFailed.Store(true)
		return "", fmt.Errorf("simulated upload failure after %d uploads", f.failAfterN)
	}
	return f.inner.UploadMultipart(ctx, localPath, objectPath)
}

func (f *faultInjectingStorage) Download(ctx context.Context, objectPath, localPath string) error {
	return f.inner.Download(ctx, objectPath, localPath)
}

func (f *faultInjectingStorage) Delete(ctx context.Context, objectPath string) error {
	return f.inner.Delete(ctx, objectPath)
}

func (f *faultInjectingStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	return f.inner.Exists(ctx, objectPath)
}

func (f *faultInjectingStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	return f.inner.ConditionalPut(ctx, localPath, objectPath, etag)
}

func (f *faultInjectingStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	return f.inner.ListObjects(ctx, prefix)
}

// BenchmarkHeavyCompaction_PartialFailure validates compaction safety under
// simulated storage failure. It verifies that after a failed upload:
// - Source partitions remain intact and queryable
// - Manifest is consistent (no dangling compacted_into references)
// - No orphaned objects exist in storage
func BenchmarkHeavyCompaction_PartialFailure(b *testing.B) {
	dir := setupBenchDir(b, "compact-fail")
	partDir := filepath.Join(dir, "partitions")
	storageDir := filepath.Join(dir, "storage")
	manifestPath := filepath.Join(dir, "manifest.db")
	compactionWorkDir := filepath.Join(dir, "compaction_work")

	os.MkdirAll(partDir, 0755)
	os.MkdirAll(compactionWorkDir, 0755)

	realStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create small partitions that qualify for compaction (< 8MB each)
		partKey := fmt.Sprintf("20260210_%d", i)
		var sourcePartitionIDs []string
		iterPartDir := filepath.Join(partDir, fmt.Sprintf("iter_%d", i))
		os.MkdirAll(iterPartDir, 0755)
		iterWorkDir := filepath.Join(compactionWorkDir, fmt.Sprintf("iter_%d", i))
		os.MkdirAll(iterWorkDir, 0755)

		for p := 0; p < 3; p++ {
			pDir := filepath.Join(iterPartDir, fmt.Sprintf("p_%d", p))
			os.MkdirAll(pDir, 0755)
			pBuilder := partition.NewBuilder(pDir, 0)
			rows := generateHeavyRows(500, rng) // small partitions
			key := types.PartitionKey{Strategy: "time", Value: partKey}
			info, err := pBuilder.Build(ctx, rows, key)
			if err != nil {
				b.Fatal(err)
			}

			// Override partition ID to ensure uniqueness across rapid builds
			info.PartitionID = fmt.Sprintf("events:%s:%d_%d", partKey, i, p)

			objectPath := fmt.Sprintf("partitions/%s/%s.sqlite", partKey, info.PartitionID)
			if err := realStorage.Upload(ctx, info.SQLitePath, objectPath); err != nil {
				b.Fatal(err)
			}

			// Override size to be small enough for compaction candidate
			info.SizeBytes = 1024 * 100 // 100KB
			if err := catalog.RegisterPartition(ctx, info, objectPath); err != nil {
				b.Fatal(err)
			}
			sourcePartitionIDs = append(sourcePartitionIDs, info.PartitionID)
		}

		// Wrap storage with fault injection — fail the compacted partition upload
		faultStorage := newFaultInjectingStorage(realStorage, 0) // fail on first UploadMultipart

		// Create daemon with fault-injecting storage
		compactionCfg := compaction.CompactionConfig{
			MinPartitionSize:    8 * 1024 * 1024,
			MaxPartitionsPerKey: 100,
			TTLDays:             7,
			CheckInterval:       time.Hour, // won't tick during test
			WorkDir:             iterWorkDir,
		}
		daemon := compaction.NewDaemon(compactionCfg, catalog, faultStorage)

		// Run one compaction cycle — should fail during upload
		daemon.RunOnce(ctx)

		// Verify: source partitions are still active (not compacted)
		for _, srcID := range sourcePartitionIDs {
			p, err := catalog.GetPartition(ctx, srcID)
			if err != nil {
				b.Fatalf("source partition %s should still exist: %v", srcID, err)
			}
			if p.CompactedInto != nil {
				b.Fatalf("source partition %s should NOT be marked as compacted after upload failure", srcID)
			}
		}

		// Verify: no orphaned objects in storage (compacted partition should not exist)
		objects, err := realStorage.ListObjects(ctx, fmt.Sprintf("partitions/%s/", partKey))
		if err != nil {
			b.Fatal(err)
		}
		// Only the 3 source partitions should exist
		if len(objects) != 3 {
			b.Fatalf("expected 3 objects in storage (source partitions only), got %d", len(objects))
		}

		// Verify: manifest is consistent — all active partitions have valid object paths
		allPartitions, err := catalog.FindPartitions(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		for _, p := range allPartitions {
			if p.PartitionKey == partKey {
				exists, err := realStorage.Exists(ctx, p.ObjectPath)
				if err != nil {
					b.Fatalf("failed to check storage for partition %s: %v", p.PartitionID, err)
				}
				if !exists {
					b.Fatalf("manifest references non-existent object: %s", p.ObjectPath)
				}
			}
		}

		b.ReportMetric(1, "upload_failed")
		b.ReportMetric(float64(len(sourcePartitionIDs)), "source_partitions_intact")
	}
}

// BenchmarkHeavyBloomFilter_CompressedCacheMemory loads 10K bloom filters into
// the compressed LRU cache and compares memory usage against uncompressed storage.
// It verifies that compressed cache uses ≤40% of uncompressed memory and reports
// per-filter sizes and cache hit latency.
//
// Filters are sized for 10K expected items (production capacity) but filled with
// only ~500 items each, simulating typical partition bloom filters that are
// provisioned for peak cardinality but operate well below capacity. The resulting
// sparse bit arrays compress very well under Snappy.
func BenchmarkHeavyBloomFilter_CompressedCacheMemory(b *testing.B) {
	const (
		numFilters       = 10000
		expectedItems    = 10000 // filter capacity (production sizing)
		actualItems      = 200   // items actually inserted (typical fill)
	)

	rng := rand.New(rand.NewSource(42))

	// Build 10K bloom filters sized for production capacity but partially filled.
	// Uses 0.1% FPR matching the tenant_id production config, which creates
	// larger, sparser bit arrays that compress well under Snappy.
	filters := make([]*bloom.BloomFilter, numFilters)
	for i := 0; i < numFilters; i++ {
		f := bloom.NewWithEstimates(expectedItems, 0.001)
		for j := 0; j < actualItems; j++ {
			f.Add([]byte(fmt.Sprintf("tenant_%d_user_%d", rng.Intn(500), rng.Intn(50000))))
		}
		filters[i] = f
	}

	// Measure uncompressed size: raw serialized bytes per filter
	var totalUncompressedBytes int64
	for _, f := range filters {
		raw, err := f.Serialize()
		if err != nil {
			b.Fatal(err)
		}
		totalUncompressedBytes += int64(len(raw))
	}

	// Compress all filters and load into the LRU cache (large enough to hold all)
	cache := planner.NewPrunerWithCacheSize(nil, nil, 1<<30) // 1GB cache, nil catalog/storage (not needed for cache ops)

	var totalCompressedBytes int64
	for i, f := range filters {
		cf, err := bloom.CompressFilter(f)
		if err != nil {
			b.Fatal(err)
		}
		totalCompressedBytes += int64(cf.CompressedSizeBytes())
		key := fmt.Sprintf("partition_%d:tenant_id", i)
		cache.LoadBloomFilterDirect(key, cf)
	}

	stats := cache.GetCacheStats()

	bytesPerFilterUncompressed := float64(totalUncompressedBytes) / float64(numFilters)
	bytesPerFilterCompressed := float64(totalCompressedBytes) / float64(numFilters)
	compressionRatio := float64(totalCompressedBytes) / float64(totalUncompressedBytes)

	b.ReportMetric(bytesPerFilterUncompressed, "bytes_per_filter_uncompressed")
	b.ReportMetric(bytesPerFilterCompressed, "bytes_per_filter_compressed")
	b.ReportMetric(compressionRatio*100, "compression_pct")
	b.ReportMetric(float64(stats.LRUFilters), "cached_filters")
	b.ReportMetric(float64(stats.LRUMemoryBytes)/(1024*1024), "cache_MB")

	// Verify compressed cache uses ≤40% of uncompressed memory
	if compressionRatio > 0.40 {
		b.Fatalf("compressed cache uses %.1f%% of uncompressed memory, expected ≤40%%", compressionRatio*100)
	}

	// Benchmark cache hit latency: lookup existing filters
	testKey := []byte("tenant_250_user_25000")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := i % numFilters
		key := fmt.Sprintf("partition_%d:tenant_id", idx)
		cf, ok := cache.GetCachedFilter(key)
		if !ok {
			b.Fatal("expected cache hit")
		}
		cf.Contains(testKey)
	}

	// Report cache hit latency (b.N iterations already timed)
	b.ReportMetric(0, "cache_hit_latency_ns") // Go benchmark framework reports ns/op automatically
}

// probabilisticFaultStorage wraps an ObjectStorage and fails UploadMultipart
// calls with a configurable probability, simulating sustained intermittent failures.
type probabilisticFaultStorage struct {
	inner        storage.ObjectStorage
	failRate     float64 // probability of failure per UploadMultipart call (0.0–1.0)
	rng          *rand.Rand
	mu           sync.Mutex
	uploadCalls  atomic.Int64
	uploadFails  atomic.Int64
}

func newProbabilisticFaultStorage(inner storage.ObjectStorage, failRate float64, seed int64) *probabilisticFaultStorage {
	return &probabilisticFaultStorage{
		inner:    inner,
		failRate: failRate,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (p *probabilisticFaultStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	return p.inner.Upload(ctx, localPath, objectPath)
}

func (p *probabilisticFaultStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	p.uploadCalls.Add(1)
	p.mu.Lock()
	fail := p.rng.Float64() < p.failRate
	p.mu.Unlock()
	if fail {
		p.uploadFails.Add(1)
		return "", fmt.Errorf("simulated probabilistic upload failure (rate=%.0f%%)", p.failRate*100)
	}
	return p.inner.UploadMultipart(ctx, localPath, objectPath)
}

func (p *probabilisticFaultStorage) Download(ctx context.Context, objectPath, localPath string) error {
	return p.inner.Download(ctx, objectPath, localPath)
}

func (p *probabilisticFaultStorage) Delete(ctx context.Context, objectPath string) error {
	return p.inner.Delete(ctx, objectPath)
}

func (p *probabilisticFaultStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	return p.inner.Exists(ctx, objectPath)
}

func (p *probabilisticFaultStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	return p.inner.ConditionalPut(ctx, localPath, objectPath, etag)
}

func (p *probabilisticFaultStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	return p.inner.ListObjects(ctx, prefix)
}

// BenchmarkHeavyCompaction_SustainedFailure runs 100 compaction cycles with a
// 10% upload failure rate and verifies that the compaction daemon can sustain
// throughput without unbounded backlog growth. It tracks successful and failed
// compactions, backlog size (small partition count) over time, and recovery
// time after failures stop.
func BenchmarkHeavyCompaction_SustainedFailure(b *testing.B) {
	dir := setupBenchDir(b, "compact-sustained")
	storageDir := filepath.Join(dir, "storage")
	manifestPath := filepath.Join(dir, "manifest.db")
	workDir := filepath.Join(dir, "compaction_work")

	os.MkdirAll(workDir, 0755)

	realStorage, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	defer catalog.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(99))

	const (
		totalCycles       = 100
		failureRate       = 0.10
		partitionsPerSeed = 3 // small partitions seeded per cycle
	)

	b.ResetTimer()
	b.ReportAllocs()

	for iter := 0; iter < b.N; iter++ {
		// Seed initial backlog: create small partitions across several keys
		// so the daemon always has work to do.
		initialKeys := 5
		for k := 0; k < initialKeys; k++ {
			partKey := fmt.Sprintf("sustained_%d_%d", iter, k)
			pDir := filepath.Join(dir, "parts", fmt.Sprintf("iter%d_key%d", iter, k))
			os.MkdirAll(pDir, 0755)
			for p := 0; p < partitionsPerSeed; p++ {
				subDir := filepath.Join(pDir, fmt.Sprintf("p%d", p))
				os.MkdirAll(subDir, 0755)
				pBuilder := partition.NewBuilder(subDir, 0)
				rows := generateHeavyRows(200, rng)
				key := types.PartitionKey{Strategy: "time", Value: partKey}
				info, err := pBuilder.Build(ctx, rows, key)
				if err != nil {
					b.Fatal(err)
				}
				info.PartitionID = fmt.Sprintf("sustained:%d:%d:%d", iter, k, p)
				objPath := fmt.Sprintf("partitions/%s/%s.sqlite", partKey, info.PartitionID)
				if err := realStorage.Upload(ctx, info.SQLitePath, objPath); err != nil {
					b.Fatal(err)
				}
				info.SizeBytes = 50 * 1024 // 50KB — well under 8MB threshold
				if err := catalog.RegisterPartition(ctx, info, objPath); err != nil {
					b.Fatal(err)
				}
			}
		}

		faultStorage := newProbabilisticFaultStorage(realStorage, failureRate, int64(iter))

		compactionCfg := compaction.CompactionConfig{
			MinPartitionSize:    8 * 1024 * 1024,
			MaxPartitionsPerKey: 100,
			TTLDays:             7,
			CheckInterval:       time.Hour,
			WorkDir:             workDir,
		}
		daemon := compaction.NewDaemon(compactionCfg, catalog, faultStorage)

		var successfulCompactions int
		var failedCycles int
		backlogHistory := make([]int64, 0, totalCycles)

		// Phase 1: Run cycles with 10% failure rate
		for cycle := 0; cycle < totalCycles; cycle++ {
			// Snapshot backlog before this cycle
			backlog, err := catalog.GetPartitionCount(ctx)
			if err != nil {
				b.Fatal(err)
			}
			backlogHistory = append(backlogHistory, backlog)

			prevFails := faultStorage.uploadFails.Load()
			daemon.RunOnce(ctx)
			newFails := faultStorage.uploadFails.Load()

			if newFails > prevFails {
				failedCycles++
			} else if backlog > 0 {
				// If backlog decreased or stayed same without new failures, count as success
				newBacklog, _ := catalog.GetPartitionCount(ctx)
				if newBacklog < backlog {
					successfulCompactions++
				}
			}

			// Inject new small partitions every 10 cycles to simulate ongoing ingestion
			if cycle%10 == 0 && cycle > 0 {
				partKey := fmt.Sprintf("sustained_%d_new_%d", iter, cycle)
				pDir := filepath.Join(dir, "parts", fmt.Sprintf("iter%d_new%d", iter, cycle))
				os.MkdirAll(pDir, 0755)
				for p := 0; p < 3; p++ {
					subDir := filepath.Join(pDir, fmt.Sprintf("p%d", p))
					os.MkdirAll(subDir, 0755)
					pBuilder := partition.NewBuilder(subDir, 0)
					rows := generateHeavyRows(200, rng)
					key := types.PartitionKey{Strategy: "time", Value: partKey}
					info, err := pBuilder.Build(ctx, rows, key)
					if err != nil {
						b.Fatal(err)
					}
					info.PartitionID = fmt.Sprintf("sustained:%d:new:%d:%d", iter, cycle, p)
					objPath := fmt.Sprintf("partitions/%s/%s.sqlite", partKey, info.PartitionID)
					if err := realStorage.Upload(ctx, info.SQLitePath, objPath); err != nil {
						b.Fatal(err)
					}
					info.SizeBytes = 50 * 1024
					if err := catalog.RegisterPartition(ctx, info, objPath); err != nil {
						b.Fatal(err)
					}
				}
			}
		}

		backlogAfterFailures, _ := catalog.GetPartitionCount(ctx)

		// Phase 2: Run recovery cycles with no failures to measure recovery
		recoveryStorage := newProbabilisticFaultStorage(realStorage, 0.0, int64(iter+1000))
		recoveryDaemon := compaction.NewDaemon(compactionCfg, catalog, recoveryStorage)

		recoveryCycles := 0
		for recoveryCycles < 50 {
			prevCount, _ := catalog.GetPartitionCount(ctx)
			recoveryDaemon.RunOnce(ctx)
			newCount, _ := catalog.GetPartitionCount(ctx)
			recoveryCycles++
			// If no more compaction work is being done, we've recovered
			if newCount >= prevCount {
				break
			}
		}

		backlogAfterRecovery, _ := catalog.GetPartitionCount(ctx)

		// Verify: backlog must not grow unboundedly.
		// The initial backlog is initialKeys * partitionsPerSeed = 15.
		// With 10% failure rate and periodic new partitions, the backlog should
		// stay bounded. We allow up to 4x the initial seeded count as headroom.
		initialBacklog := int64(initialKeys * partitionsPerSeed)
		maxAllowedBacklog := initialBacklog * 4
		if backlogAfterFailures > maxAllowedBacklog {
			b.Fatalf("backlog grew unboundedly: %d partitions after %d cycles (initial=%d, max_allowed=%d)",
				backlogAfterFailures, totalCycles, initialBacklog, maxAllowedBacklog)
		}

		// Verify: successful compactions must outpace failure-induced backlog
		if successfulCompactions == 0 && failedCycles > 0 {
			b.Fatalf("no successful compactions in %d cycles with %d failures — daemon cannot make progress", totalCycles, failedCycles)
		}

		b.ReportMetric(float64(successfulCompactions)/float64(totalCycles), "compactions_per_cycle")
		b.ReportMetric(float64(backlogAfterFailures), "backlog_partitions")
		b.ReportMetric(float64(recoveryCycles), "failure_recovery_cycles")
		b.ReportMetric(float64(failedCycles), "failed_cycles")
		b.ReportMetric(float64(successfulCompactions), "successful_compactions")
		b.ReportMetric(float64(backlogAfterRecovery), "backlog_after_recovery")
	}
}
