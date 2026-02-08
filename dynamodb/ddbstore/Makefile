PHONY: gen test

install:
	go install github.com/mna/pigeon@latest

gen:
	pigeon -o conditionexpr/parser/parser_gen.go conditionexpr/parser/cond_expr.peg
	pigeon -o keyconditionexpr/parser/parser_gen.go keyconditionexpr/parser/key_cond_expr.peg
	pigeon -o updateexpr/parser/parser_gen.go updateexpr/parser/update_expr.peg
	pigeon -o projectionexpr/parser/parser_gen.go projectionexpr/parser/projection_expr.peg

test:
	go test ./...
