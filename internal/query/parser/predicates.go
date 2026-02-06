package parser

// PredicateType represents the type of a predicate.
type PredicateType int

const (
	PredicateEquality PredicateType = iota // column = value
	PredicateRange                         // column < value, column > value, etc.
	PredicateIn                            // column IN (v1, v2, ...)
	PredicateBetween                       // column BETWEEN low AND high
	PredicateLike                          // column LIKE pattern
	PredicateIsNull                        // column IS NULL / IS NOT NULL
)

// Predicate represents an extracted predicate from a WHERE clause.
type Predicate struct {
	Type     PredicateType
	Column   string        // Column name
	Table    string        // Optional table qualifier
	Operator string        // =, <, >, <=, >=, <>, IN, BETWEEN, LIKE, IS NULL
	Value    interface{}   // Single value for equality/range
	Values   []interface{} // Multiple values for IN
	Low      interface{}   // Low bound for BETWEEN
	High     interface{}   // High bound for BETWEEN
	Not      bool          // Negation flag (NOT IN, NOT LIKE, IS NOT NULL)
}

// PredicateExtractor extracts predicates from WHERE clauses.
type PredicateExtractor struct {
	predicates []Predicate
	columns    map[string]bool // Set of columns referenced in predicates
}

// NewPredicateExtractor creates a new PredicateExtractor.
func NewPredicateExtractor() *PredicateExtractor {
	return &PredicateExtractor{
		columns: make(map[string]bool),
	}
}

// ExtractPredicates extracts all predicates from a SELECT statement.
func ExtractPredicates(stmt *SelectStatement) []Predicate {
	if stmt.Where == nil {
		return nil
	}
	extractor := NewPredicateExtractor()
	extractor.extract(stmt.Where)
	return extractor.predicates
}

// GetPredicateColumns returns all columns referenced in predicates.
func GetPredicateColumns(stmt *SelectStatement) []string {
	if stmt.Where == nil {
		return nil
	}
	extractor := NewPredicateExtractor()
	extractor.extract(stmt.Where)

	columns := make([]string, 0, len(extractor.columns))
	for col := range extractor.columns {
		columns = append(columns, col)
	}
	return columns
}

// extract recursively extracts predicates from an expression.
func (e *PredicateExtractor) extract(expr Expression) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		e.extractBinary(ex)
	case *InExpr:
		e.extractIn(ex)
	case *BetweenExpr:
		e.extractBetween(ex)
	case *LikeExpr:
		e.extractLike(ex)
	case *IsNullExpr:
		e.extractIsNull(ex)
	case *UnaryExpr:
		// For NOT expressions, recurse into the operand
		if ex.Operator == "NOT" {
			e.extract(ex.Operand)
		}
	case *ParenExpr:
		e.extract(ex.Expr)
	}
}

// extractBinary extracts predicates from a binary expression.
func (e *PredicateExtractor) extractBinary(expr *BinaryExpr) {
	switch expr.Operator {
	case "AND", "OR":
		// Recurse into both sides
		e.extract(expr.Left)
		e.extract(expr.Right)
	case "=", "<>", "!=":
		e.extractComparison(expr, PredicateEquality)
	case "<", ">", "<=", ">=":
		e.extractComparison(expr, PredicateRange)
	}
}

// extractComparison extracts a comparison predicate.
func (e *PredicateExtractor) extractComparison(expr *BinaryExpr, predType PredicateType) {
	// Check if left side is a column reference
	if col, ok := expr.Left.(*ColumnRef); ok {
		if val := e.extractValue(expr.Right); val != nil {
			pred := Predicate{
				Type:     predType,
				Column:   col.Column,
				Table:    col.Table,
				Operator: expr.Operator,
				Value:    val,
			}
			e.predicates = append(e.predicates, pred)
			e.addColumn(col)
			return
		}
	}

	// Check if right side is a column reference (reverse comparison)
	if col, ok := expr.Right.(*ColumnRef); ok {
		if val := e.extractValue(expr.Left); val != nil {
			// Reverse the operator for range predicates
			op := expr.Operator
			switch op {
			case "<":
				op = ">"
			case ">":
				op = "<"
			case "<=":
				op = ">="
			case ">=":
				op = "<="
			}
			pred := Predicate{
				Type:     predType,
				Column:   col.Column,
				Table:    col.Table,
				Operator: op,
				Value:    val,
			}
			e.predicates = append(e.predicates, pred)
			e.addColumn(col)
		}
	}
}

// extractIn extracts an IN predicate.
func (e *PredicateExtractor) extractIn(expr *InExpr) {
	col, ok := expr.Expr.(*ColumnRef)
	if !ok {
		return
	}

	var values []interface{}
	for _, v := range expr.Values {
		if val := e.extractValue(v); val != nil {
			values = append(values, val)
		}
	}

	if len(values) > 0 {
		pred := Predicate{
			Type:     PredicateIn,
			Column:   col.Column,
			Table:    col.Table,
			Operator: "IN",
			Values:   values,
			Not:      expr.Not,
		}
		e.predicates = append(e.predicates, pred)
		e.addColumn(col)
	}
}

// extractBetween extracts a BETWEEN predicate.
func (e *PredicateExtractor) extractBetween(expr *BetweenExpr) {
	col, ok := expr.Expr.(*ColumnRef)
	if !ok {
		return
	}

	low := e.extractValue(expr.Low)
	high := e.extractValue(expr.High)

	if low != nil && high != nil {
		pred := Predicate{
			Type:     PredicateBetween,
			Column:   col.Column,
			Table:    col.Table,
			Operator: "BETWEEN",
			Low:      low,
			High:     high,
			Not:      expr.Not,
		}
		e.predicates = append(e.predicates, pred)
		e.addColumn(col)
	}
}

// extractLike extracts a LIKE predicate.
func (e *PredicateExtractor) extractLike(expr *LikeExpr) {
	col, ok := expr.Expr.(*ColumnRef)
	if !ok {
		return
	}

	pattern := e.extractValue(expr.Pattern)
	if pattern != nil {
		pred := Predicate{
			Type:     PredicateLike,
			Column:   col.Column,
			Table:    col.Table,
			Operator: "LIKE",
			Value:    pattern,
			Not:      expr.Not,
		}
		e.predicates = append(e.predicates, pred)
		e.addColumn(col)
	}
}

// extractIsNull extracts an IS NULL predicate.
func (e *PredicateExtractor) extractIsNull(expr *IsNullExpr) {
	col, ok := expr.Expr.(*ColumnRef)
	if !ok {
		return
	}

	pred := Predicate{
		Type:     PredicateIsNull,
		Column:   col.Column,
		Table:    col.Table,
		Operator: "IS NULL",
		Not:      expr.Not,
	}
	e.predicates = append(e.predicates, pred)
	e.addColumn(col)
}

// extractValue extracts a literal value from an expression.
func (e *PredicateExtractor) extractValue(expr Expression) interface{} {
	switch ex := expr.(type) {
	case *Literal:
		return ex.Value
	case *ParenExpr:
		return e.extractValue(ex.Expr)
	case *UnaryExpr:
		// Handle negative numbers
		if ex.Operator == "-" {
			if val := e.extractValue(ex.Operand); val != nil {
				switch v := val.(type) {
				case int64:
					return -v
				case float64:
					return -v
				}
			}
		}
	}
	return nil
}

// addColumn adds a column to the set of referenced columns.
func (e *PredicateExtractor) addColumn(col *ColumnRef) {
	key := col.Column
	if col.Table != "" {
		key = col.Table + "." + col.Column
	}
	e.columns[key] = true
}

// FilterPredicatesByColumn returns predicates for a specific column.
func FilterPredicatesByColumn(predicates []Predicate, column string) []Predicate {
	var result []Predicate
	for _, p := range predicates {
		if p.Column == column {
			result = append(result, p)
		}
	}
	return result
}

// FilterPredicatesByType returns predicates of a specific type.
func FilterPredicatesByType(predicates []Predicate, predType PredicateType) []Predicate {
	var result []Predicate
	for _, p := range predicates {
		if p.Type == predType {
			result = append(result, p)
		}
	}
	return result
}

// GetEqualityPredicates returns all equality predicates (useful for bloom filter pruning).
func GetEqualityPredicates(predicates []Predicate) []Predicate {
	return FilterPredicatesByType(predicates, PredicateEquality)
}

// GetRangePredicates returns all range predicates (useful for min/max pruning).
func GetRangePredicates(predicates []Predicate) []Predicate {
	var result []Predicate
	for _, p := range predicates {
		if p.Type == PredicateRange || p.Type == PredicateBetween {
			result = append(result, p)
		}
	}
	return result
}

// CanUseBoolFilter returns true if the predicate can use bloom filter pruning.
func CanUseBloomFilter(p Predicate) bool {
	// Bloom filters are useful for equality and IN predicates
	return (p.Type == PredicateEquality && p.Operator == "=") ||
		(p.Type == PredicateIn && !p.Not)
}

// CanUseMinMaxPruning returns true if the predicate can use min/max pruning.
func CanUseMinMaxPruning(p Predicate) bool {
	// Min/max pruning works for range and BETWEEN predicates
	return p.Type == PredicateRange || p.Type == PredicateBetween ||
		(p.Type == PredicateEquality && p.Operator == "=")
}
