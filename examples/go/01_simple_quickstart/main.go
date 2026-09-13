package main

import (
	"fmt"
	"os"

	"github.com/arkiliandb/Arkilian/bindings/go/arkilian"
)

const dbPath = "quickstart.sqlite"

func cleanupDB(path string) {
	for _, ext := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + ext)
	}
}

func main() {
	fmt.Println("=== Arkilian Go SDK: Simple Quickstart ===")
	fmt.Println()

	cleanupDB(dbPath)
	defer cleanupDB(dbPath)

	// In quickstart embedded mode, disable cloud backup warnings
	os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")

	// 1. Open embedded database
	fmt.Printf("[1] Initializing Arkilian embedded database at '%s'...\n", dbPath)
	db, err := arkilian.OpenDB(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 2. Create schema
	fmt.Println("[2] Creating schema...")
	schemaSQL := `
		CREATE TABLE IF NOT EXISTS products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sku TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			price REAL NOT NULL,
			stock INTEGER NOT NULL DEFAULT 0
		);
	`
	if err := db.Exec(schemaSQL); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create schema: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("    ✓ Table 'products' created.")

	// 3. Insert records with prepared statement
	fmt.Println("[3] Inserting products with prepared statement...")
	insertSQL := "INSERT INTO products (sku, name, price, stock) VALUES (?, ?, ?, ?)"
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to prepare insert statement: %v\n", err)
		os.Exit(1)
	}

	products := []struct {
		sku   string
		name  string
		price float64
		stock int
	}{
		{"GO-101", "Gopher Plush Toy", 24.95, 150},
		{"GO-102", "Concurrency in Go Book", 39.99, 85},
		{"GO-103", "Mechanical Keyboard (Go Blue)", 149.00, 30},
	}

	for _, p := range products {
		if err := stmt.Reset(); err != nil {
			fmt.Fprintf(os.Stderr, "Statement reset error: %v\n", err)
			os.Exit(1)
		}
		_ = stmt.BindText(1, p.sku)
		_ = stmt.BindText(2, p.name)
		_ = stmt.BindDouble(3, p.price)
		_ = stmt.BindInt(4, p.stock)
		if _, err := stmt.Step(); err != nil {
			fmt.Fprintf(os.Stderr, "Insert step error: %v\n", err)
			os.Exit(1)
		}
	}
	_ = stmt.Finalize()
	fmt.Printf("    ✓ %d products successfully inserted.\n", len(products))

	// 4. Query all products
	fmt.Println("\n[4] Querying all products:")
	querySQL := "SELECT id, sku, name, price, stock FROM products ORDER BY id ASC"
	qStmt, err := db.Prepare(querySQL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to prepare query statement: %v\n", err)
		os.Exit(1)
	}

	for {
		hasRow, err := qStmt.Step()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query step error: %v\n", err)
			break
		}
		if !hasRow {
			break
		}
		id := qStmt.ColumnInt64(0)
		sku := qStmt.ColumnText(1)
		name := qStmt.ColumnText(2)
		price := qStmt.ColumnDouble(3)
		stock := qStmt.ColumnInt(4)
		fmt.Printf("    - ID %d: %s [%s] | $%.2f | In Stock: %d\n", id, name, sku, price, stock)
	}
	_ = qStmt.Finalize()

	// 5. Atomic transaction demonstration
	fmt.Println("\n[5] Executing atomic stock adjustment transaction...")
	if err := db.Begin(); err != nil {
		fmt.Fprintf(os.Stderr, "Begin failed: %v\n", err)
		os.Exit(1)
	}

	err1 := db.Exec("UPDATE products SET stock = stock - 10 WHERE sku = 'GO-101'")
	err2 := db.Exec("UPDATE products SET stock = stock + 15 WHERE sku = 'GO-103'")
	if err1 != nil || err2 != nil {
		_ = db.Rollback()
		fmt.Println("    ✗ Transaction rolled back due to error.")
	} else {
		if err := db.Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "Commit failed: %v\n", err)
		} else {
			fmt.Println("    ✓ Transaction committed successfully.")
		}
	}

	// 6. Verify updated stock
	fmt.Println("\n[6] Verifying updated stock:")
	vStmt, _ := db.Prepare("SELECT sku, stock FROM products WHERE sku IN ('GO-101', 'GO-103') ORDER BY sku ASC")
	for {
		hasRow, _ := vStmt.Step()
		if !hasRow {
			break
		}
		fmt.Printf("    - %s: %d units remaining\n", vStmt.ColumnText(0), vStmt.ColumnInt(1))
	}
	_ = vStmt.Finalize()

	fmt.Println("\n[7] Closing database connection cleanly.")
	fmt.Println("\n=== Quickstart Completed Successfully ===")
}
