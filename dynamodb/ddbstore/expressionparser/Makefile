PHONY: gen test

gen:
	pigeon -o conditionparser/parser_gen.go conditionparser/cond_expr.peg
	pigeon -o keyconditionparser/parser_gen.go keyconditionparser/key_cond_expr.peg

test:
	go test ./...
