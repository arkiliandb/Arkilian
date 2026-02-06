package parser

import (
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{
			"SELECT * FROM events",
			[]TokenType{TokenSelect, TokenStar, TokenFrom, TokenIdent, TokenEOF},
		},
		{
			"SELECT id, name FROM users WHERE id = 1",
			[]TokenType{TokenSelect, TokenIdent, TokenComma, TokenIdent, TokenFrom, TokenIdent, TokenWhere, TokenIdent, TokenEq, TokenNumber, TokenEOF},
		},
		{
			"SELECT COUNT(*) FROM events WHERE tenant_id = 'acme'",
			[]TokenType{TokenSelect, TokenCount, TokenLParen, TokenStar, TokenRParen, TokenFrom, TokenIdent, TokenWhere, TokenIdent, TokenEq, TokenString, TokenEOF},
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tokens := lexer.Tokenize()

		if len(tokens) != len(tt.expected) {
			t.Errorf("input %q: expected %d tokens, got %d", tt.input, len(tt.expected), len(tokens))
			continue
		}

		for i, tok := range tokens {
			if tok.Type != tt.expected[i] {
				t.Errorf("input %q: token %d: expected %s, got %s", tt.input, i, tt.expected[i], tok.Type)
			}
		}
	}
}

func TestParseSimpleSelect(t *testing.T) {
	input := "SELECT * FROM events"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(sel.Columns))
	}

	if sel.From == nil || sel.From.Name != "events" {
		t.Errorf("expected FROM events, got %v", sel.From)
	}
}

func TestParseSelectWithWhere(t *testing.T) {
	input := "SELECT id, name FROM users WHERE id = 1"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(sel.Columns))
	}

	if sel.Where == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseSelectWithGroupBy(t *testing.T) {
	input := "SELECT tenant_id, COUNT(*) FROM events GROUP BY tenant_id"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.GroupBy) != 1 {
		t.Errorf("expected 1 GROUP BY column, got %d", len(sel.GroupBy))
	}
}

func TestParseSelectWithOrderBy(t *testing.T) {
	input := "SELECT * FROM events ORDER BY event_time DESC"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY clause, got %d", len(sel.OrderBy))
	}

	if !sel.OrderBy[0].Desc {
		t.Error("expected DESC ordering")
	}
}

func TestParseSelectWithLimit(t *testing.T) {
	input := "SELECT * FROM events LIMIT 10"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %v", sel.Limit)
	}
}

func TestParseAggregates(t *testing.T) {
	tests := []struct {
		input    string
		funcName string
	}{
		{"SELECT COUNT(*) FROM events", "COUNT"},
		{"SELECT SUM(amount) FROM orders", "SUM"},
		{"SELECT AVG(price) FROM products", "AVG"},
		{"SELECT MIN(created_at) FROM users", "MIN"},
		{"SELECT MAX(updated_at) FROM users", "MAX"},
	}

	for _, tt := range tests {
		stmt, err := Parse(tt.input)
		if err != nil {
			t.Errorf("input %q: unexpected error: %v", tt.input, err)
			continue
		}

		sel, ok := stmt.(*SelectStatement)
		if !ok {
			t.Errorf("input %q: expected SelectStatement", tt.input)
			continue
		}

		if len(sel.Columns) != 1 {
			t.Errorf("input %q: expected 1 column", tt.input)
			continue
		}

		agg, ok := sel.Columns[0].Expr.(*AggregateExpr)
		if !ok {
			t.Errorf("input %q: expected AggregateExpr, got %T", tt.input, sel.Columns[0].Expr)
			continue
		}

		if agg.Function != tt.funcName {
			t.Errorf("input %q: expected function %s, got %s", tt.input, tt.funcName, agg.Function)
		}
	}
}

func TestParseInExpression(t *testing.T) {
	input := "SELECT * FROM events WHERE tenant_id IN ('acme', 'corp', 'test')"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	inExpr, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}

	if len(inExpr.Values) != 3 {
		t.Errorf("expected 3 values in IN clause, got %d", len(inExpr.Values))
	}
}

func TestParseBetweenExpression(t *testing.T) {
	input := "SELECT * FROM events WHERE event_time BETWEEN 1000 AND 2000"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	betweenExpr, ok := sel.Where.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", sel.Where)
	}

	low, ok := betweenExpr.Low.(*Literal)
	if !ok || low.Value.(int64) != 1000 {
		t.Errorf("expected low bound 1000, got %v", betweenExpr.Low)
	}

	high, ok := betweenExpr.High.(*Literal)
	if !ok || high.Value.(int64) != 2000 {
		t.Errorf("expected high bound 2000, got %v", betweenExpr.High)
	}
}

func TestExtractPredicates(t *testing.T) {
	input := "SELECT * FROM events WHERE tenant_id = 'acme' AND user_id = 123"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	predicates := ExtractPredicates(sel)

	if len(predicates) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(predicates))
	}

	// Check first predicate
	found := false
	for _, p := range predicates {
		if p.Column == "tenant_id" && p.Value == "acme" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected predicate for tenant_id = 'acme'")
	}

	// Check second predicate
	found = false
	for _, p := range predicates {
		if p.Column == "user_id" && p.Value == int64(123) {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected predicate for user_id = 123")
	}
}

func TestExtractRangePredicates(t *testing.T) {
	input := "SELECT * FROM events WHERE event_time >= 1000 AND event_time < 2000"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	predicates := ExtractPredicates(sel)

	rangePredicates := GetRangePredicates(predicates)
	if len(rangePredicates) != 2 {
		t.Fatalf("expected 2 range predicates, got %d", len(rangePredicates))
	}
}

func TestASTString(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"SELECT * FROM events"},
		{"SELECT id, name FROM users WHERE id = 1"},
		{"SELECT COUNT(*) FROM events GROUP BY tenant_id"},
		{"SELECT * FROM events ORDER BY event_time DESC LIMIT 10"},
	}

	for _, tt := range tests {
		stmt, err := Parse(tt.input)
		if err != nil {
			t.Errorf("input %q: unexpected error: %v", tt.input, err)
			continue
		}

		// The String() method should produce valid SQL
		sql := stmt.String()
		if sql == "" {
			t.Errorf("input %q: String() returned empty string", tt.input)
		}

		// Parse the generated SQL to verify it's valid
		_, err = Parse(sql)
		if err != nil {
			t.Errorf("input %q: generated SQL %q failed to parse: %v", tt.input, sql, err)
		}
	}
}

func TestParseError(t *testing.T) {
	tests := []string{
		"SELEC * FROM events",        // Typo in SELECT
		"SELECT FROM events",         // Missing columns
		"SELECT * FROM",              // Missing table name
		"SELECT * FROM events WHERE", // Incomplete WHERE
	}

	for _, input := range tests {
		_, err := Parse(input)
		if err == nil {
			t.Errorf("input %q: expected error, got nil", input)
		}
	}
}

func TestComplexQuery(t *testing.T) {
	input := `SELECT tenant_id, user_id, COUNT(*) as cnt, SUM(amount) as total
              FROM events
              WHERE tenant_id = 'acme' AND event_time BETWEEN 1000 AND 2000
              GROUP BY tenant_id, user_id
              HAVING COUNT(*) > 10
              ORDER BY total DESC
              LIMIT 100`

	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(sel.Columns))
	}

	if sel.From == nil || sel.From.Name != "events" {
		t.Error("expected FROM events")
	}

	if sel.Where == nil {
		t.Error("expected WHERE clause")
	}

	if len(sel.GroupBy) != 2 {
		t.Errorf("expected 2 GROUP BY columns, got %d", len(sel.GroupBy))
	}

	if sel.Having == nil {
		t.Error("expected HAVING clause")
	}

	if len(sel.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY clause, got %d", len(sel.OrderBy))
	}

	if sel.Limit == nil || *sel.Limit != 100 {
		t.Errorf("expected LIMIT 100, got %v", sel.Limit)
	}
}
