package parser

import (
	"testing"
)

func TestParseNotInExpression(t *testing.T) {
input := "SELECT * FROM events WHERE tenant_id NOT IN ('acme', 'corp')"
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

if !inExpr.Not {
t.Error("expected NOT IN")
}
}

func TestParseNotLikeExpression(t *testing.T) {
input := "SELECT * FROM events WHERE name NOT LIKE 'test%'"
stmt, err := Parse(input)
if err != nil {
t.Fatalf("unexpected error: %v", err)
}

sel, ok := stmt.(*SelectStatement)
if !ok {
t.Fatalf("expected SelectStatement, got %T", stmt)
}

likeExpr, ok := sel.Where.(*LikeExpr)
if !ok {
t.Fatalf("expected LikeExpr, got %T", sel.Where)
}

if !likeExpr.Not {
t.Error("expected NOT LIKE")
}
}

func TestParseNotBetweenExpression(t *testing.T) {
input := "SELECT * FROM events WHERE event_time NOT BETWEEN 1000 AND 2000"
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

if !betweenExpr.Not {
t.Error("expected NOT BETWEEN")
}
}

func TestExtractNotInPredicate(t *testing.T) {
input := "SELECT * FROM events WHERE tenant_id NOT IN ('acme', 'corp')"
stmt, err := Parse(input)
if err != nil {
t.Fatalf("unexpected error: %v", err)
}

sel := stmt.(*SelectStatement)
predicates := ExtractPredicates(sel)

if len(predicates) != 1 {
t.Fatalf("expected 1 predicate, got %d", len(predicates))
}

if predicates[0].Operator != "IN" || !predicates[0].Not {
t.Errorf("expected NOT IN predicate, got %+v", predicates[0])
}
}

func TestExtractNotLikePredicate(t *testing.T) {
input := "SELECT * FROM events WHERE name NOT LIKE 'test%'"
stmt, err := Parse(input)
if err != nil {
t.Fatalf("unexpected error: %v", err)
}

sel := stmt.(*SelectStatement)
predicates := ExtractPredicates(sel)

if len(predicates) != 1 {
t.Fatalf("expected 1 predicate, got %d", len(predicates))
}

if predicates[0].Operator != "LIKE" || !predicates[0].Not {
t.Errorf("expected NOT LIKE predicate, got %+v", predicates[0])
}
}

func TestExtractNotBetweenPredicate(t *testing.T) {
input := "SELECT * FROM events WHERE event_time NOT BETWEEN 1000 AND 2000"
stmt, err := Parse(input)
if err != nil {
t.Fatalf("unexpected error: %v", err)
}

sel := stmt.(*SelectStatement)
predicates := ExtractPredicates(sel)

if len(predicates) != 1 {
t.Fatalf("expected 1 predicate, got %d", len(predicates))
}

if predicates[0].Operator != "BETWEEN" || !predicates[0].Not {
t.Errorf("expected NOT BETWEEN predicate, got %+v", predicates[0])
}
}
