PHONY: gen test

gen:
	pigeon -o parser/parser_gen.go parser/cond_expr.peg

test:
	go test ./...
