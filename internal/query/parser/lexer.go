// Package parser provides SQL parsing for the Arkilian query federation layer.
package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of a lexical token.
type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenError
	TokenIdent
	TokenNumber
	TokenString

	// Keywords
	TokenSelect
	TokenFrom
	TokenWhere
	TokenGroupBy
	TokenOrderBy
	TokenLimit
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenBetween
	TokenAs
	TokenAsc
	TokenDesc
	TokenNull
	TokenIs
	TokenLike
	TokenDistinct
	TokenBy
	TokenHaving
	TokenOffset

	// Aggregate functions
	TokenCount
	TokenSum
	TokenAvg
	TokenMin
	TokenMax

	// Operators
	TokenEq       // =
	TokenNe       // <> or !=
	TokenLt       // <
	TokenGt       // >
	TokenLe       // <=
	TokenGe       // >=
	TokenPlus     // +
	TokenMinus    // -
	TokenStar     // *
	TokenSlash    // /
	TokenComma    // ,
	TokenLParen   // (
	TokenRParen   // )
	TokenDot      // .
	TokenSemicolon // ;
)

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // Position in input
}

// String returns a string representation of the token.
func (t Token) String() string {
	return fmt.Sprintf("Token{%s, %q, %d}", t.Type.String(), t.Literal, t.Pos)
}

// String returns the string representation of a TokenType.
func (t TokenType) String() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenError:
		return "ERROR"
	case TokenIdent:
		return "IDENT"
	case TokenNumber:
		return "NUMBER"
	case TokenString:
		return "STRING"
	case TokenSelect:
		return "SELECT"
	case TokenFrom:
		return "FROM"
	case TokenWhere:
		return "WHERE"
	case TokenGroupBy:
		return "GROUP BY"
	case TokenOrderBy:
		return "ORDER BY"
	case TokenLimit:
		return "LIMIT"
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenNot:
		return "NOT"
	case TokenIn:
		return "IN"
	case TokenBetween:
		return "BETWEEN"
	case TokenAs:
		return "AS"
	case TokenAsc:
		return "ASC"
	case TokenDesc:
		return "DESC"
	case TokenNull:
		return "NULL"
	case TokenIs:
		return "IS"
	case TokenLike:
		return "LIKE"
	case TokenDistinct:
		return "DISTINCT"
	case TokenBy:
		return "BY"
	case TokenHaving:
		return "HAVING"
	case TokenOffset:
		return "OFFSET"
	case TokenCount:
		return "COUNT"
	case TokenSum:
		return "SUM"
	case TokenAvg:
		return "AVG"
	case TokenMin:
		return "MIN"
	case TokenMax:
		return "MAX"
	case TokenEq:
		return "="
	case TokenNe:
		return "<>"
	case TokenLt:
		return "<"
	case TokenGt:
		return ">"
	case TokenLe:
		return "<="
	case TokenGe:
		return ">="
	case TokenPlus:
		return "+"
	case TokenMinus:
		return "-"
	case TokenStar:
		return "*"
	case TokenSlash:
		return "/"
	case TokenComma:
		return ","
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenDot:
		return "."
	case TokenSemicolon:
		return ";"
	default:
		return "UNKNOWN"
	}
}

// keywords maps SQL keywords to their token types.
var keywords = map[string]TokenType{
	"SELECT":   TokenSelect,
	"FROM":     TokenFrom,
	"WHERE":    TokenWhere,
	"GROUP":    TokenGroupBy, // Will be combined with BY
	"ORDER":    TokenOrderBy, // Will be combined with BY
	"LIMIT":    TokenLimit,
	"AND":      TokenAnd,
	"OR":       TokenOr,
	"NOT":      TokenNot,
	"IN":       TokenIn,
	"BETWEEN":  TokenBetween,
	"AS":       TokenAs,
	"ASC":      TokenAsc,
	"DESC":     TokenDesc,
	"NULL":     TokenNull,
	"IS":       TokenIs,
	"LIKE":     TokenLike,
	"DISTINCT": TokenDistinct,
	"BY":       TokenBy,
	"HAVING":   TokenHaving,
	"OFFSET":   TokenOffset,
	"COUNT":    TokenCount,
	"SUM":      TokenSum,
	"AVG":      TokenAvg,
	"MIN":      TokenMin,
	"MAX":      TokenMax,
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // Current position in input
	readPos int  // Reading position (after current char)
	ch      byte // Current character
}

// NewLexer creates a new Lexer for the given input.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character and advances the position.
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

// peekChar returns the next character without advancing.
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// skipWhitespace skips whitespace characters.
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	startPos := l.pos
	var tok Token

	switch l.ch {
	case '=':
		tok = Token{Type: TokenEq, Literal: "=", Pos: startPos}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenLe, Literal: "<=", Pos: startPos}
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TokenNe, Literal: "<>", Pos: startPos}
		} else {
			tok = Token{Type: TokenLt, Literal: "<", Pos: startPos}
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenGe, Literal: ">=", Pos: startPos}
		} else {
			tok = Token{Type: TokenGt, Literal: ">", Pos: startPos}
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenNe, Literal: "!=", Pos: startPos}
		} else {
			tok = Token{Type: TokenError, Literal: string(l.ch), Pos: startPos}
		}
	case '+':
		tok = Token{Type: TokenPlus, Literal: "+", Pos: startPos}
	case '-':
		tok = Token{Type: TokenMinus, Literal: "-", Pos: startPos}
	case '*':
		tok = Token{Type: TokenStar, Literal: "*", Pos: startPos}
	case '/':
		tok = Token{Type: TokenSlash, Literal: "/", Pos: startPos}
	case ',':
		tok = Token{Type: TokenComma, Literal: ",", Pos: startPos}
	case '(':
		tok = Token{Type: TokenLParen, Literal: "(", Pos: startPos}
	case ')':
		tok = Token{Type: TokenRParen, Literal: ")", Pos: startPos}
	case '.':
		tok = Token{Type: TokenDot, Literal: ".", Pos: startPos}
	case ';':
		tok = Token{Type: TokenSemicolon, Literal: ";", Pos: startPos}
	case '\'':
		tok = l.readString()
	case 0:
		tok = Token{Type: TokenEOF, Literal: "", Pos: startPos}
	default:
		if isLetter(l.ch) || l.ch == '_' {
			return l.readIdentifier()
		} else if isDigit(l.ch) {
			return l.readNumber()
		} else {
			tok = Token{Type: TokenError, Literal: string(l.ch), Pos: startPos}
		}
	}

	l.readChar()
	return tok
}

// readIdentifier reads an identifier or keyword.
func (l *Lexer) readIdentifier() Token {
	startPos := l.pos
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	literal := l.input[start:l.pos]
	upper := strings.ToUpper(literal)

	// Check for keywords
	if tokType, ok := keywords[upper]; ok {
		return Token{Type: tokType, Literal: upper, Pos: startPos}
	}

	return Token{Type: TokenIdent, Literal: literal, Pos: startPos}
}

// readNumber reads a numeric literal.
func (l *Lexer) readNumber() Token {
	startPos := l.pos
	start := l.pos
	hasDecimal := false

	for isDigit(l.ch) || (l.ch == '.' && !hasDecimal) {
		if l.ch == '.' {
			hasDecimal = true
		}
		l.readChar()
	}

	return Token{Type: TokenNumber, Literal: l.input[start:l.pos], Pos: startPos}
}

// readString reads a string literal enclosed in single quotes.
func (l *Lexer) readString() Token {
	startPos := l.pos
	l.readChar() // Skip opening quote
	start := l.pos

	for l.ch != '\'' && l.ch != 0 {
		if l.ch == '\'' && l.peekChar() == '\'' {
			// Escaped quote
			l.readChar()
		}
		l.readChar()
	}

	if l.ch == 0 {
		return Token{Type: TokenError, Literal: "unterminated string", Pos: startPos}
	}

	literal := l.input[start:l.pos]
	// Don't call readChar here - it will be called by NextToken
	return Token{Type: TokenString, Literal: literal, Pos: startPos}
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF || tok.Type == TokenError {
			break
		}
	}
	return tokens
}

// isLetter returns true if the character is a letter.
func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

// isDigit returns true if the character is a digit.
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
