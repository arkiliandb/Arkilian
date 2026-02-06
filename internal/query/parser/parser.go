package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseError represents a parsing error with location information.
type ParseError struct {
	Message  string
	Position int
	Token    Token
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s (got %s)", e.Position, e.Message, e.Token.Literal)
}

// Parser parses SQL statements into AST.
type Parser struct {
	lexer     *Lexer
	curToken  Token
	peekToken Token
	errors    []error
}

// NewParser creates a new Parser for the given input.
func NewParser(input string) *Parser {
	p := &Parser{
		lexer: NewLexer(input),
	}
	// Read two tokens to initialize curToken and peekToken
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the input and returns a Statement.
func Parse(input string) (Statement, error) {
	p := NewParser(input)
	return p.ParseStatement()
}

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// curTokenIs checks if the current token is of the given type.
func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

// peekTokenIs checks if the peek token is of the given type.
func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

// expectPeek advances if the peek token matches, otherwise returns an error.
func (p *Parser) expectPeek(t TokenType) error {
	if p.peekTokenIs(t) {
		p.nextToken()
		return nil
	}
	return &ParseError{
		Message:  fmt.Sprintf("expected %s", t.String()),
		Position: p.peekToken.Pos,
		Token:    p.peekToken,
	}
}

// ParseStatement parses a SQL statement.
func (p *Parser) ParseStatement() (Statement, error) {
	switch p.curToken.Type {
	case TokenSelect:
		return p.parseSelectStatement()
	default:
		return nil, &ParseError{
			Message:  "expected SELECT",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
}

// parseSelectStatement parses a SELECT statement.
func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// Skip SELECT
	p.nextToken()

	// Check for DISTINCT
	if p.curTokenIs(TokenDistinct) {
		stmt.Distinct = true
		p.nextToken()
	}

	// Parse columns
	columns, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// Parse FROM clause
	if p.curTokenIs(TokenFrom) {
		p.nextToken()
		tableRef, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.From = tableRef
	}

	// Parse WHERE clause
	if p.curTokenIs(TokenWhere) {
		p.nextToken()
		where, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse GROUP BY clause
	if p.curTokenIs(TokenGroupBy) {
		p.nextToken()
		// Expect BY after GROUP
		if p.curTokenIs(TokenBy) {
			p.nextToken()
		}
		groupBy, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// Parse HAVING clause
	if p.curTokenIs(TokenHaving) {
		p.nextToken()
		having, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	// Parse ORDER BY clause
	if p.curTokenIs(TokenOrderBy) {
		p.nextToken()
		// Expect BY after ORDER
		if p.curTokenIs(TokenBy) {
			p.nextToken()
		}
		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Parse LIMIT clause
	if p.curTokenIs(TokenLimit) {
		p.nextToken()
		if !p.curTokenIs(TokenNumber) {
			return nil, &ParseError{
				Message:  "expected number after LIMIT",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		limit, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			return nil, &ParseError{
				Message:  "invalid LIMIT value",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		stmt.Limit = &limit
		p.nextToken()
	}

	// Parse OFFSET clause
	if p.curTokenIs(TokenOffset) {
		p.nextToken()
		if !p.curTokenIs(TokenNumber) {
			return nil, &ParseError{
				Message:  "expected number after OFFSET",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		offset, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			return nil, &ParseError{
				Message:  "invalid OFFSET value",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		stmt.Offset = &offset
		p.nextToken()
	}

	return stmt, nil
}

// parseSelectColumns parses the column list in a SELECT statement.
func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var columns []SelectColumn

	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)

		if !p.curTokenIs(TokenComma) {
			break
		}
		p.nextToken() // Skip comma
	}

	return columns, nil
}

// parseSelectColumn parses a single column in the SELECT clause.
func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	col := SelectColumn{}

	// Check for *
	if p.curTokenIs(TokenStar) {
		col.Expr = &StarExpr{}
		p.nextToken()
		return col, nil
	}

	// Parse expression
	expr, err := p.parseExpression(0)
	if err != nil {
		return col, err
	}
	col.Expr = expr

	// Check for alias
	if p.curTokenIs(TokenAs) {
		p.nextToken()
		if !p.curTokenIs(TokenIdent) {
			return col, &ParseError{
				Message:  "expected identifier after AS",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		col.Alias = p.curToken.Literal
		p.nextToken()
	} else if p.curTokenIs(TokenIdent) {
		// Alias without AS
		col.Alias = p.curToken.Literal
		p.nextToken()
	}

	return col, nil
}

// parseTableRef parses a table reference.
func (p *Parser) parseTableRef() (*TableRef, error) {
	if !p.curTokenIs(TokenIdent) {
		return nil, &ParseError{
			Message:  "expected table name",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}

	ref := &TableRef{Name: p.curToken.Literal}
	p.nextToken()

	// Check for alias
	if p.curTokenIs(TokenAs) {
		p.nextToken()
		if !p.curTokenIs(TokenIdent) {
			return nil, &ParseError{
				Message:  "expected identifier after AS",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		ref.Alias = p.curToken.Literal
		p.nextToken()
	} else if p.curTokenIs(TokenIdent) {
		// Alias without AS
		ref.Alias = p.curToken.Literal
		p.nextToken()
	}

	return ref, nil
}

// parseExpressionList parses a comma-separated list of expressions.
func (p *Parser) parseExpressionList() ([]Expression, error) {
	var exprs []Expression

	for {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if !p.curTokenIs(TokenComma) {
			break
		}
		p.nextToken() // Skip comma
	}

	return exprs, nil
}

// parseOrderByList parses the ORDER BY clause items.
func (p *Parser) parseOrderByList() ([]OrderByClause, error) {
	var clauses []OrderByClause

	for {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}

		clause := OrderByClause{Expr: expr}

		// Check for ASC/DESC
		if p.curTokenIs(TokenAsc) {
			p.nextToken()
		} else if p.curTokenIs(TokenDesc) {
			clause.Desc = true
			p.nextToken()
		}

		clauses = append(clauses, clause)

		if !p.curTokenIs(TokenComma) {
			break
		}
		p.nextToken() // Skip comma
	}

	return clauses, nil
}

// Operator precedence levels
const (
	precLowest  = 0
	precOr      = 1
	precAnd     = 2
	precNot     = 3
	precCompare = 4
	precAdd     = 5
	precMul     = 6
	precUnary   = 7
)

// getPrecedence returns the precedence of the current token.
func (p *Parser) getPrecedence() int {
	switch p.curToken.Type {
	case TokenOr:
		return precOr
	case TokenAnd:
		return precAnd
	case TokenEq, TokenNe, TokenLt, TokenGt, TokenLe, TokenGe, TokenLike, TokenIn, TokenBetween, TokenIs:
		return precCompare
	case TokenPlus, TokenMinus:
		return precAdd
	case TokenStar, TokenSlash:
		return precMul
	default:
		return precLowest
	}
}

// parseExpression parses an expression with operator precedence.
func (p *Parser) parseExpression(precedence int) (Expression, error) {
	// Parse prefix expression
	left, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}

	// Parse infix expressions
	for !p.curTokenIs(TokenEOF) && precedence < p.getPrecedence() {
		left, err = p.parseInfixExpression(left)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

// parsePrefixExpression parses a prefix expression.
func (p *Parser) parsePrefixExpression() (Expression, error) {
	switch p.curToken.Type {
	case TokenIdent:
		return p.parseIdentifierOrFunction()
	case TokenNumber:
		return p.parseNumber()
	case TokenString:
		return p.parseString()
	case TokenNull:
		p.nextToken()
		return &Literal{Value: nil}, nil
	case TokenLParen:
		return p.parseGroupedExpression()
	case TokenNot:
		return p.parseNotExpression()
	case TokenMinus:
		return p.parseUnaryMinus()
	case TokenCount, TokenSum, TokenAvg, TokenMin, TokenMax:
		return p.parseAggregate()
	case TokenStar:
		star := &StarExpr{}
		p.nextToken()
		return star, nil
	default:
		return nil, &ParseError{
			Message:  "unexpected token in expression",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
}

// parseIdentifierOrFunction parses an identifier or function call.
func (p *Parser) parseIdentifierOrFunction() (Expression, error) {
	name := p.curToken.Literal
	p.nextToken()

	// Check for table.column
	if p.curTokenIs(TokenDot) {
		p.nextToken()
		if p.curTokenIs(TokenStar) {
			// table.*
			star := &StarExpr{Table: name}
			p.nextToken()
			return star, nil
		}
		if !p.curTokenIs(TokenIdent) {
			return nil, &ParseError{
				Message:  "expected column name after dot",
				Position: p.curToken.Pos,
				Token:    p.curToken,
			}
		}
		col := &ColumnRef{Table: name, Column: p.curToken.Literal}
		p.nextToken()
		return col, nil
	}

	// Check for function call
	if p.curTokenIs(TokenLParen) {
		return p.parseFunctionCall(name)
	}

	// Simple column reference
	return &ColumnRef{Column: name}, nil
}

// parseFunctionCall parses a function call.
func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	p.nextToken() // Skip (

	// Check for aggregate functions
	upperName := strings.ToUpper(name)
	if upperName == "COUNT" || upperName == "SUM" || upperName == "AVG" || upperName == "MIN" || upperName == "MAX" {
		return p.parseAggregateArgs(upperName)
	}

	// Regular function call
	var args []Expression
	if !p.curTokenIs(TokenRParen) {
		for {
			arg, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)

			if !p.curTokenIs(TokenComma) {
				break
			}
			p.nextToken()
		}
	}

	if !p.curTokenIs(TokenRParen) {
		return nil, &ParseError{
			Message:  "expected ) after function arguments",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return &FunctionCall{Name: name, Args: args}, nil
}

// parseAggregate parses an aggregate function.
func (p *Parser) parseAggregate() (Expression, error) {
	funcName := p.curToken.Literal
	p.nextToken()

	if !p.curTokenIs(TokenLParen) {
		return nil, &ParseError{
			Message:  "expected ( after aggregate function",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return p.parseAggregateArgs(funcName)
}

// parseAggregateArgs parses the arguments of an aggregate function.
func (p *Parser) parseAggregateArgs(funcName string) (Expression, error) {
	agg := &AggregateExpr{Function: funcName}

	// Check for DISTINCT
	if p.curTokenIs(TokenDistinct) {
		agg.Distinct = true
		p.nextToken()
	}

	// Check for * (COUNT(*))
	if p.curTokenIs(TokenStar) {
		agg.Arg = &StarExpr{}
		p.nextToken()
	} else if !p.curTokenIs(TokenRParen) {
		arg, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		agg.Arg = arg
	}

	if !p.curTokenIs(TokenRParen) {
		return nil, &ParseError{
			Message:  "expected ) after aggregate argument",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return agg, nil
}

// parseNumber parses a numeric literal.
func (p *Parser) parseNumber() (Expression, error) {
	literal := p.curToken.Literal
	p.nextToken()

	// Try parsing as int64 first
	if !strings.Contains(literal, ".") {
		val, err := strconv.ParseInt(literal, 10, 64)
		if err == nil {
			return &Literal{Value: val}, nil
		}
	}

	// Parse as float64
	val, err := strconv.ParseFloat(literal, 64)
	if err != nil {
		return nil, &ParseError{
			Message:  "invalid number",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	return &Literal{Value: val}, nil
}

// parseString parses a string literal.
func (p *Parser) parseString() (Expression, error) {
	// Handle escaped quotes
	val := strings.ReplaceAll(p.curToken.Literal, "''", "'")
	p.nextToken()
	return &Literal{Value: val}, nil
}

// parseGroupedExpression parses a parenthesized expression.
func (p *Parser) parseGroupedExpression() (Expression, error) {
	p.nextToken() // Skip (

	expr, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}

	if !p.curTokenIs(TokenRParen) {
		return nil, &ParseError{
			Message:  "expected )",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return &ParenExpr{Expr: expr}, nil
}

// parseNotExpression parses a NOT expression.
func (p *Parser) parseNotExpression() (Expression, error) {
	p.nextToken() // Skip NOT

	expr, err := p.parseExpression(precNot)
	if err != nil {
		return nil, err
	}

	return &UnaryExpr{Operator: "NOT", Operand: expr}, nil
}

// parseUnaryMinus parses a unary minus expression.
func (p *Parser) parseUnaryMinus() (Expression, error) {
	p.nextToken() // Skip -

	expr, err := p.parseExpression(precUnary)
	if err != nil {
		return nil, err
	}

	return &UnaryExpr{Operator: "-", Operand: expr}, nil
}

// parseInfixExpression parses an infix expression.
func (p *Parser) parseInfixExpression(left Expression) (Expression, error) {
	switch p.curToken.Type {
	case TokenAnd, TokenOr:
		return p.parseBinaryExpression(left)
	case TokenEq, TokenNe, TokenLt, TokenGt, TokenLe, TokenGe:
		return p.parseBinaryExpression(left)
	case TokenPlus, TokenMinus, TokenStar, TokenSlash:
		return p.parseBinaryExpression(left)
	case TokenLike:
		return p.parseLikeExpression(left, false)
	case TokenIn:
		return p.parseInExpression(left, false)
	case TokenBetween:
		return p.parseBetweenExpression(left, false)
	case TokenIs:
		return p.parseIsExpression(left)
	case TokenNot:
		return p.parseNotInfix(left)
	default:
		return left, nil
	}
}

// parseBinaryExpression parses a binary expression.
func (p *Parser) parseBinaryExpression(left Expression) (Expression, error) {
	op := p.curToken.Literal
	precedence := p.getPrecedence()
	p.nextToken()

	right, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}

	return &BinaryExpr{Left: left, Operator: op, Right: right}, nil
}

// parseLikeExpression parses a LIKE expression.
func (p *Parser) parseLikeExpression(left Expression, not bool) (Expression, error) {
	p.nextToken() // Skip LIKE

	pattern, err := p.parseExpression(precCompare)
	if err != nil {
		return nil, err
	}

	return &LikeExpr{Expr: left, Pattern: pattern, Not: not}, nil
}

// parseInExpression parses an IN expression.
func (p *Parser) parseInExpression(left Expression, not bool) (Expression, error) {
	p.nextToken() // Skip IN

	if !p.curTokenIs(TokenLParen) {
		return nil, &ParseError{
			Message:  "expected ( after IN",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	var values []Expression
	for {
		val, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		values = append(values, val)

		if !p.curTokenIs(TokenComma) {
			break
		}
		p.nextToken()
	}

	if !p.curTokenIs(TokenRParen) {
		return nil, &ParseError{
			Message:  "expected ) after IN values",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return &InExpr{Expr: left, Values: values, Not: not}, nil
}

// parseBetweenExpression parses a BETWEEN expression.
func (p *Parser) parseBetweenExpression(left Expression, not bool) (Expression, error) {
	p.nextToken() // Skip BETWEEN

	low, err := p.parseExpression(precCompare)
	if err != nil {
		return nil, err
	}

	if !p.curTokenIs(TokenAnd) {
		return nil, &ParseError{
			Message:  "expected AND in BETWEEN expression",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	high, err := p.parseExpression(precCompare)
	if err != nil {
		return nil, err
	}

	return &BetweenExpr{Expr: left, Low: low, High: high, Not: not}, nil
}

// parseIsExpression parses an IS NULL or IS NOT NULL expression.
func (p *Parser) parseIsExpression(left Expression) (Expression, error) {
	p.nextToken() // Skip IS

	not := false
	if p.curTokenIs(TokenNot) {
		not = true
		p.nextToken()
	}

	if !p.curTokenIs(TokenNull) {
		return nil, &ParseError{
			Message:  "expected NULL after IS",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
	p.nextToken()

	return &IsNullExpr{Expr: left, Not: not}, nil
}

// parseNotInfix parses NOT IN, NOT LIKE, NOT BETWEEN.
func (p *Parser) parseNotInfix(left Expression) (Expression, error) {
	p.nextToken() // Skip NOT

	switch p.curToken.Type {
	case TokenIn:
		return p.parseInExpression(left, true)
	case TokenLike:
		return p.parseLikeExpression(left, true)
	case TokenBetween:
		return p.parseBetweenExpression(left, true)
	default:
		return nil, &ParseError{
			Message:  "expected IN, LIKE, or BETWEEN after NOT",
			Position: p.curToken.Pos,
			Token:    p.curToken,
		}
	}
}
