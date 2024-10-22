package main

import (
	"bezos/bzoddb/expressionparser/calculator/ast"
	"testing"
)

var longishExpr = `
18 + 3 - 27012 * ( (1234 - 43) / 7 ) + -4 * 8129
`

var validCases = map[string]int{
	"0":   0,
	"1":   1,
	"-1":  -1,
	"10":  10,
	"-10": -10,

	"(0)":   0,
	"(1)":   1,
	"(-1)":  -1,
	"(10)":  10,
	"(-10)": -10,

	"1+1":   2,
	"1-1":   0,
	"1*1":   1,
	"1/1":   1,
	"1 + 1": 2,
	"1 - 1": 0,
	"1 * 1": 1,
	"1 / 1": 1,

	"1+0":   1,
	"1-0":   1,
	"1*0":   0,
	"1 + 0": 1,
	"1 - 0": 1,
	"1 * 0": 0,

	"1\n+\t2\r\n +\n3\n": 6,
	"(2) * 3":            6,

	" 1 + 2 - 3 * 4 / 5 ":       1,
	" 1 + (2 - 3) * 4 / 5 ":     1,
	" (1 + 2 - 3) * 4 / 5 ":     0,
	" 1 + 2 - (3 * 4) / 5 ":     1,
	" 18 + 3 - 27 * (-18 / -3)": -141,
	longishExpr:                 -4624535,
}

func TestValidCases(t *testing.T) {
	for tc, exp := range validCases {
		parsed, err := Parse("", []byte(tc))
		if err != nil {
			t.Errorf("%q: want no error, got %v", tc, err)
			continue
		}
		calc, ok := parsed.(ast.Node)
		if !ok {
			t.Errorf("expected ast.Node, got %T", parsed)
			continue
		}
		v := calc.Eval()
		if exp != v {
			t.Errorf("%q: want %d, got %d", tc, exp, v)
		}
	}
}

var invalidCases = map[string]string{
	"":   `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"(":  `1:2 (1): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	")":  `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"()": `1:2 (1): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"+":  `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"-":  `1:2 (1): no match found, expected: [0-9]`,
	"*":  `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"/":  `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"+1": `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"*1": `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"/1": `1:1 (0): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	// not a parser error since we defer this to the AST evaluation contrary to the
	// approach in pigeon https://github.com/mna/pigeon/blob/v1.3.0/examples/calculator/calculator.peg
	// "1/0":     "1:4 (3): rule Term: runtime error: integer divide by zero",
	"1+":      `1:3 (2): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"1-":      `1:3 (2): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"1*":      `1:3 (2): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"1/":      `1:3 (2): no match found, expected: "(", "-", [ \n\t\r] or [0-9]`,
	"1 (+ 2)": `1:3 (2): no match found, expected: "*", "+", "-", "/", [ \n\t\r] or EOF`,
	"1 (2)":   `1:3 (2): no match found, expected: "*", "+", "-", "/", [ \n\t\r] or EOF`,
	"\xfe":    "1:1 (0): invalid encoding",
}

func TestInvalidCases(t *testing.T) {
	for tc, exp := range invalidCases {
		parsed, err := Parse("", []byte(tc))
		if err == nil {
			t.Errorf("%q: want error, got none (%v)", tc, parsed)
			continue
		}
		el, ok := err.(errList)
		if !ok {
			t.Errorf("%q: want error type %T, got %T", tc, &errList{}, err)
			continue
		}
		for _, e := range el {
			if _, ok := e.(*parserError); !ok {
				t.Errorf("%q: want all individual errors to be %T, got %T (%[3]v)", tc, &parserError{}, e)
			}
		}
		if exp != err.Error() {
			t.Errorf("%q: want \n%s\n, got \n%s\n", tc, exp, err)
		}
	}
}

func TestPanicNoRecover(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			// all good
			return
		}
		t.Fatal("want panic, got none")
	}()

	// should panic
	parsed, err := Parse("", []byte("1 / 0"), Recover(false))
	if err != nil {
		t.Errorf("expcted no error, got %v", err)
	}
	calc, ok := parsed.(ast.Node)
	if !ok {
		t.Errorf("expected ast.Node, got %T", parsed)
	}

	calc.Eval()
}

func TestMemoization(t *testing.T) {
	in := " 2 + 35 * ( 18 - -4 / ( 5 + 1) ) * 456 + -1"
	want := 287281

	p := newParser("", []byte(in), Memoize(false))
	parsed, err := p.parse(g)
	if err != nil {
		t.Fatal(err)
	}
	calc, ok := parsed.(ast.Node)
	if !ok {
		t.Errorf("expected ast.Node, got %T", parsed)
	}
	v := calc.Eval()
	if v != want {
		t.Errorf("want %d, got %d", want, v)
	}
	if p.ExprCnt != 473 {
		t.Errorf("with Memoize=false, want %d expressions evaluated, got %d", 473, p.ExprCnt)
	}

	p = newParser("", []byte(in), Memoize(true))
	parsed, err = p.parse(g)
	if err != nil {
		t.Fatal(err)
	}
	calc, ok = parsed.(ast.Node)
	if !ok {
		t.Errorf("expected ast.Node, got %T", parsed)
	}
	v = calc.Eval()
	if v != want {
		t.Errorf("want %d, got %d", want, v)
	}
	if p.ExprCnt != 447 {
		t.Errorf("with Memoize=true, want %d expressions evaluated, got %d", 447, p.ExprCnt)
	}
}

func BenchmarkPigeonCalculatorNoMemo(b *testing.B) {
	d := []byte(longishExpr)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := Parse("", d, Memoize(false)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPigeonCalculatorMemo(b *testing.B) {
	d := []byte(longishExpr)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := Parse("", d, Memoize(true)); err != nil {
			b.Fatal(err)
		}
	}
}
