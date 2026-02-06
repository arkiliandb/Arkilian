package parser

import (
	"fmt"
	"strings"
)

// Statement represents a parsed SQL statement.
type Statement interface {
	statementNode()
	String() string
}

// Expression represents an expression in the AST.
type Expression interface {
	expressionNode()
	String() string
}

// SelectStatement represents a SELECT query.
type SelectStatement struct {
	Distinct bool
	Columns  []SelectColumn
	From     *TableRef
	Where    Expression
	GroupBy  []Expression
	Having   Expression
	OrderBy  []OrderByClause
	Limit    *int64
	Offset   *int64
}

func (s *SelectStatement) statementNode() {}

// String returns the SQL representation of the SELECT statement.
func (s *SelectStatement) String() string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	if s.Distinct {
		sb.WriteString("DISTINCT ")
	}

	// Columns
	cols := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		cols[i] = col.String()
	}
	sb.WriteString(strings.Join(cols, ", "))

	// FROM
	if s.From != nil {
		sb.WriteString(" FROM ")
		sb.WriteString(s.From.String())
	}

	// WHERE
	if s.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(s.Where.String())
	}

	// GROUP BY
	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		groups := make([]string, len(s.GroupBy))
		for i, g := range s.GroupBy {
			groups[i] = g.String()
		}
		sb.WriteString(strings.Join(groups, ", "))
	}

	// HAVING
	if s.Having != nil {
		sb.WriteString(" HAVING ")
		sb.WriteString(s.Having.String())
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		orders := make([]string, len(s.OrderBy))
		for i, o := range s.OrderBy {
			orders[i] = o.String()
		}
		sb.WriteString(strings.Join(orders, ", "))
	}

	// LIMIT
	if s.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *s.Limit))
	}

	// OFFSET
	if s.Offset != nil {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", *s.Offset))
	}

	return sb.String()
}

// SelectColumn represents a column in the SELECT clause.
type SelectColumn struct {
	Expr  Expression
	Alias string
}

// String returns the SQL representation of the select column.
func (c SelectColumn) String() string {
	if c.Alias != "" {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias)
	}
	return c.Expr.String()
}

// TableRef represents a table reference in the FROM clause.
type TableRef struct {
	Name  string
	Alias string
}

// String returns the SQL representation of the table reference.
func (t *TableRef) String() string {
	if t.Alias != "" {
		return fmt.Sprintf("%s AS %s", t.Name, t.Alias)
	}
	return t.Name
}

// OrderByClause represents an ORDER BY clause item.
type OrderByClause struct {
	Expr Expression
	Desc bool
}

// String returns the SQL representation of the ORDER BY clause.
func (o OrderByClause) String() string {
	if o.Desc {
		return fmt.Sprintf("%s DESC", o.Expr.String())
	}
	return fmt.Sprintf("%s ASC", o.Expr.String())
}

// BinaryExpr represents a binary operation (e.g., a = b, a > b).
type BinaryExpr struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (b *BinaryExpr) expressionNode() {}

// String returns the SQL representation of the binary expression.
func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Operator, b.Right.String())
}

// UnaryExpr represents a unary operation (e.g., NOT x, -x).
type UnaryExpr struct {
	Operator string
	Operand  Expression
}

func (u *UnaryExpr) expressionNode() {}

// String returns the SQL representation of the unary expression.
func (u *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", u.Operator, u.Operand.String())
}

// ColumnRef represents a column reference.
type ColumnRef struct {
	Table  string
	Column string
}

func (c *ColumnRef) expressionNode() {}

// String returns the SQL representation of the column reference.
func (c *ColumnRef) String() string {
	if c.Table != "" {
		return fmt.Sprintf("%s.%s", c.Table, c.Column)
	}
	return c.Column
}

// Literal represents a literal value.
type Literal struct {
	Value interface{}
}

func (l *Literal) expressionNode() {}

// String returns the SQL representation of the literal.
func (l *Literal) String() string {
	switch v := l.Value.(type) {
	case string:
		// Escape single quotes
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case nil:
		return "NULL"
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// AggregateExpr represents an aggregate function call.
type AggregateExpr struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Arg      Expression
	Distinct bool
}

func (a *AggregateExpr) expressionNode() {}

// String returns the SQL representation of the aggregate expression.
func (a *AggregateExpr) String() string {
	var sb strings.Builder
	sb.WriteString(a.Function)
	sb.WriteString("(")
	if a.Distinct {
		sb.WriteString("DISTINCT ")
	}
	if a.Arg != nil {
		sb.WriteString(a.Arg.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// StarExpr represents the * wildcard in SELECT *.
type StarExpr struct {
	Table string // Optional table qualifier (e.g., t.*)
}

func (s *StarExpr) expressionNode() {}

// String returns the SQL representation of the star expression.
func (s *StarExpr) String() string {
	if s.Table != "" {
		return fmt.Sprintf("%s.*", s.Table)
	}
	return "*"
}

// FunctionCall represents a function call expression.
type FunctionCall struct {
	Name string
	Args []Expression
}

func (f *FunctionCall) expressionNode() {}

// String returns the SQL representation of the function call.
func (f *FunctionCall) String() string {
	args := make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", f.Name, strings.Join(args, ", "))
}

// InExpr represents an IN expression (e.g., x IN (1, 2, 3)).
type InExpr struct {
	Expr   Expression
	Values []Expression
	Not    bool
}

func (i *InExpr) expressionNode() {}

// String returns the SQL representation of the IN expression.
func (i *InExpr) String() string {
	values := make([]string, len(i.Values))
	for j, v := range i.Values {
		values[j] = v.String()
	}
	if i.Not {
		return fmt.Sprintf("%s NOT IN (%s)", i.Expr.String(), strings.Join(values, ", "))
	}
	return fmt.Sprintf("%s IN (%s)", i.Expr.String(), strings.Join(values, ", "))
}

// BetweenExpr represents a BETWEEN expression (e.g., x BETWEEN 1 AND 10).
type BetweenExpr struct {
	Expr Expression
	Low  Expression
	High Expression
	Not  bool
}

func (b *BetweenExpr) expressionNode() {}

// String returns the SQL representation of the BETWEEN expression.
func (b *BetweenExpr) String() string {
	if b.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", b.Expr.String(), b.Low.String(), b.High.String())
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", b.Expr.String(), b.Low.String(), b.High.String())
}

// IsNullExpr represents an IS NULL or IS NOT NULL expression.
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (i *IsNullExpr) expressionNode() {}

// String returns the SQL representation of the IS NULL expression.
func (i *IsNullExpr) String() string {
	if i.Not {
		return fmt.Sprintf("%s IS NOT NULL", i.Expr.String())
	}
	return fmt.Sprintf("%s IS NULL", i.Expr.String())
}

// LikeExpr represents a LIKE expression.
type LikeExpr struct {
	Expr    Expression
	Pattern Expression
	Not     bool
}

func (l *LikeExpr) expressionNode() {}

// String returns the SQL representation of the LIKE expression.
func (l *LikeExpr) String() string {
	if l.Not {
		return fmt.Sprintf("%s NOT LIKE %s", l.Expr.String(), l.Pattern.String())
	}
	return fmt.Sprintf("%s LIKE %s", l.Expr.String(), l.Pattern.String())
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expression
}

func (p *ParenExpr) expressionNode() {}

// String returns the SQL representation of the parenthesized expression.
func (p *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", p.Expr.String())
}
