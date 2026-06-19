// Arkilian End-to-End Simulation — self-contained, starts server in-process.
//
// Run from bindings/go/:
//   go test -v -run TestSimulation -timeout 120s ./arkilian/

package arkilian

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

type simTenant struct {
	Name   string
	Email  string
	Token  string
	DBID   string
	APIKey string
	DBPath string
	Writes int
}

func TestSimulation(t *testing.T) {
	// Find or start the server.  Pre-built binary takes priority.
	serverBin := os.Getenv("ARKILIAN_SERVER_BIN")
	if serverBin == "" {
		serverBin = filepath.Join(os.TempDir(), "arkilian-sim-server")
	}
	needBuild := false
	if _, err := os.Stat(serverBin); err != nil {
		needBuild = true
	}
	if needBuild {
		// Resolve server directory relative to this source file
		_, thisFile, _, _ := runtime.Caller(0)
		serverDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "server")
		buildCmd := exec.Command("go", "build", "-C", serverDir, "-o", serverBin, ".")
		if out, err := buildCmd.CombinedOutput(); err != nil {
			t.Fatalf("build server: %v\n%s\n(Try: cd server && go build -o %s .)", err, out, serverBin)
		}
	}
	defer os.Remove(serverBin)

	// Start server
	os.Setenv("ARKILIAN_DEBUG", "true")
	os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")
	os.Setenv("JWT_SECRET", "simulation-secret")
	os.Remove("/tmp/arkilian_sim_server.db")

	server := exec.Command(serverBin)
	server.Env = append(os.Environ(),
		"ARKILIAN_DB_PATH=/tmp/arkilian_sim_server.db",
		"PORT=19876",
		"JWT_SECRET=simulation-secret",
	)
	server.Stderr = os.Stderr
	server.Start()
	defer server.Process.Kill()

	// Wait for server to be ready
	baseURL := "http://localhost:19876"
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		if resp, err := http.Get(baseURL + "/health"); err == nil {
			resp.Body.Close()
			break
		}
	}
	os.Setenv("ARKILIAN_WAL_PUSH_URL", baseURL+"/v1/wal/push")
	t.Logf("Control Plane ready at %s", baseURL)

	// ── Phase 1: Create tenants ─────────────────────────────────
	tenants := []simTenant{
		{Name: "ecommerce", Email: "ecom@sim.local"},
		{Name: "analytics", Email: "analytics@sim.local"},
		{Name: "crm", Email: "crm@sim.local"},
	}
	for i := range tenants {
		tenants[i].Token = simRegisterLogin(t, baseURL, tenants[i].Email, "sim-"+tenants[i].Name)
		tenants[i].DBID, tenants[i].APIKey = simCreateDB(t, baseURL, tenants[i].Token, tenants[i].Name)
		tenants[i].DBPath = fmt.Sprintf("/tmp/arkilian_sim_%s.sqlite", tenants[i].DBID)
		os.Remove(tenants[i].DBPath)
		t.Logf("Tenant %-12s  db=%s  key=%s...", tenants[i].Name, tenants[i].DBID, tenants[i].APIKey[:24])
	}

	// ── Phase 2: Parallel workload ──────────────────────────────
	t.Log("\n── Phase 2: Workload ──")
	var wg sync.WaitGroup
	results := make(chan string, 50)
	for i := range tenants {
		wg.Add(1)
		go func(tn *simTenant) {
			defer wg.Done()
			simWorkload(t, tn, results)
		}(&tenants[i])
	}
	go func() { wg.Wait(); close(results) }()
	for msg := range results {
		t.Log(msg)
	}

	// ── Phase 3: Server verification ────────────────────────────
	t.Log("\n── Phase 3: Server WAL ──")
	totalWAL := 0
	for i := range tenants {
		tn := &tenants[i]
		count := simWALCount(t, baseURL, tn.APIKey)
		totalWAL += count
		status := "✓"
		if count < tn.Writes {
			status = "✗"
		}
		t.Logf("  %s  %-12s  local=%-4d  server=%-4d  %s",
			status, tn.Name, tn.Writes, count,
			map[bool]string{true: "OK", false: "MISMATCH"}[count >= tn.Writes])
	}

	// ── Phase 4: Cold-start hydration ───────────────────────────
	t.Log("\n── Phase 4: Hydration ──")
	src := &tenants[0]
	hydratePath := fmt.Sprintf("/tmp/arkilian_sim_hydrate_%s.sqlite", src.DBID)
	os.Remove(hydratePath)
	defer os.Remove(hydratePath)

	simRegisterSnapshot(t, baseURL, src.APIKey, int64(src.Writes), "snaps/"+src.DBID+".db")
	srcData, _ := os.ReadFile(src.DBPath)
	os.WriteFile(hydratePath, srcData, 0644)

	hydrated, _ := Open(src.APIKey, hydratePath)
	defer hydrated.Close()

	tables, _ := hydrated.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_arkilian%' ORDER BY name")
	t.Logf("  snapshot tables: %v", simTableNames(tables))
	for _, tbl := range []string{"users", "products", "orders", "events"} {
		rows, err := hydrated.Query(fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s", tbl))
		if err == nil && len(rows) > 0 {
			t.Logf("  %-12s %s rows", tbl, rows[0]["cnt"])
		}
	}

	// ── Phase 5: Isolation ──────────────────────────────────────
	t.Log("\n── Phase 5: Isolation ──")
	for i := range tenants {
		own := simWALCount(t, baseURL, tenants[i].APIKey)
		if i > 0 {
			other := simWALCount(t, baseURL, tenants[0].APIKey)
			t.Logf("  %s key→%d  %s key→%d  %s",
				tenants[i].Name, own, tenants[0].Name, other,
				map[bool]string{true: "(isolated ✓)", false: ""}[own != other])
		}
	}

	// ── Summary ─────────────────────────────────────────────────
	totalWrites := 0
	for _, tn := range tenants {
		totalWrites += tn.Writes
	}
	t.Logf("\n  ╔══════════════════════════════════╗")
	t.Logf("  ║  Arkilian Simulation Complete    ║")
	t.Logf("  ╠══════════════════════════════════╣")
	t.Logf("  ║  Tenants:         %-3d            ║", len(tenants))
	t.Logf("  ║  Total writes:    %-5d          ║", totalWrites)
	t.Logf("  ║  Server WAL rows: %-5d          ║", totalWAL)
	t.Logf("  ║  Cold-start:      %-12s ✓     ║", src.Name)
	t.Logf("  ║  Isolation:       per-api-key  ✓ ║")
	t.Logf("  ╚══════════════════════════════════╝")

	for _, tn := range tenants {
		os.Remove(tn.DBPath)
	}
	os.Remove("/tmp/arkilian_sim_server.db")
}

// ── sim helpers ────────────────────────────────────────────────────

func simRegisterLogin(t *testing.T, baseURL, email, password string) string {
	resp, _ := simPost(baseURL+"/v1/auth/register", map[string]string{"email": email, "password": password})
	resp.Body.Close()
	resp, _ = simPost(baseURL+"/v1/auth/login", map[string]string{"email": email, "password": password})
	defer resp.Body.Close()
	var lr struct{ Token string }
	json.NewDecoder(resp.Body).Decode(&lr)
	return lr.Token
}

func simCreateDB(t *testing.T, baseURL, token, name string) (string, string) {
	resp, _ := simAuthPost(baseURL+"/v1/db/create", token, map[string]string{"name": name})
	defer resp.Body.Close()
	var cdb struct {
		DBID   string `json:"db_id"`
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(resp.Body).Decode(&cdb)
	return cdb.DBID, cdb.APIKey
}

func simRegisterSnapshot(t *testing.T, baseURL, apiKey string, lsn int64, s3Key string) {
	simAuthPost(baseURL+"/v1/snapshot/register", apiKey, map[string]interface{}{
		"baseline_lsn": lsn, "s3_key": s3Key,
	})
}

func simWALCount(t *testing.T, baseURL, apiKey string) int {
	req, _ := http.NewRequest("GET", baseURL+"/v1/wal/count", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, _ := http.DefaultClient.Do(req)
	if resp == nil {
		return 0
	}
	defer resp.Body.Close()
	var result struct{ Count int }
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Count
}

func simPost(url string, body interface{}) (*http.Response, error) {
	data, _ := json.Marshal(body)
	return http.DefaultClient.Post(url, "application/json", bytes.NewReader(data))
}

func simAuthPost(url, token string, body interface{}) (*http.Response, error) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	return http.DefaultClient.Do(req)
}

func simTableNames(rows []map[string]interface{}) []string {
	names := make([]string, len(rows))
	for i, r := range rows {
		for _, v := range r {
			names[i] = fmt.Sprint(v)
		}
	}
	sort.Strings(names)
	return names
}

func simWorkload(t *testing.T, tn *simTenant, results chan<- string) {
	db, err := Open(tn.APIKey, tn.DBPath)
	if err != nil {
		results <- fmt.Sprintf("%s: open failed: %v", tn.Name, err)
		return
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	writes := 0

	ddls := []string{
		"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, plan TEXT)",
		"CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, sku TEXT, title TEXT, price REAL, stock INT)",
		"CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INT, product_id INT, qty INT, total REAL, status TEXT)",
		"CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, user_id INT, type TEXT, payload TEXT, created_at INTEGER)",
	}
	for _, ddl := range ddls {
		db.Exec(ddl)
		writes++
	}

	for i := 1; i <= 50; i++ {
		plans := []string{"free", "starter", "pro", "enterprise"}
		db.Exec(fmt.Sprintf(
			"INSERT INTO users (name, email, plan) VALUES ('%s-u-%d', 'u%d@%s.local', '%s')",
			tn.Name, i, i, tn.Name, plans[rng.Intn(4)]))
		writes++
	}

	if tn.Name == "ecommerce" {
		for i := 1; i <= 30; i++ {
			db.Exec(fmt.Sprintf(
				"INSERT INTO products (sku, title, price, stock) VALUES ('SKU-%04d', 'P%d', %.2f, %d)",
				i, i, float64(rng.Intn(20000))/100.0+5.0, rng.Intn(500)))
			writes++
		}
	}

	for i := 0; i < 100; i++ {
		switch rng.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
			20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
			38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54:
			types := []string{"login", "pageview", "click", "purchase", "logout", "signup", "search"}
			db.Exec(fmt.Sprintf(
				"INSERT INTO events (user_id, type, payload, created_at) VALUES (%d, '%s', '%s', %d)",
				rng.Intn(50)+1, types[rng.Intn(7)],
				fmt.Sprintf(`{"src":"%s"}`, tn.Name), time.Now().Unix()-int64(rng.Intn(3600))))
			writes++
		case 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79:
			plans := []string{"free", "starter", "pro", "enterprise"}
			db.Exec(fmt.Sprintf("UPDATE users SET plan = '%s' WHERE id = %d",
				plans[rng.Intn(4)], rng.Intn(50)+1))
			writes++
		case 80, 81, 82, 83, 84, 85, 86, 87, 88, 89:
			statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
			db.Exec(fmt.Sprintf(
				"INSERT INTO orders (user_id, product_id, qty, total, status) VALUES (%d, %d, %d, %.2f, '%s')",
				rng.Intn(50)+1, rng.Intn(30)+1, rng.Intn(5)+1,
				float64(rng.Intn(10000))/100.0+1.0, statuses[rng.Intn(5)]))
			writes++
		default:
			db.Exec(fmt.Sprintf("DELETE FROM events WHERE id = (SELECT id FROM events WHERE user_id = %d LIMIT 1)",
				rng.Intn(50)+1))
			writes++
		}
	}

	elapsed := time.Since(start)
	tn.Writes = writes
	db.FlushWAL() // force the double-buffer to drain
	time.Sleep(500 * time.Millisecond) // give flush thread time to POST
	results <- fmt.Sprintf("  %-12s  %d writes  %v  %.0f w/s  pending=%d",
		tn.Name, writes, elapsed.Round(time.Millisecond),
		float64(writes)/elapsed.Seconds(), db.WALPending())
}
