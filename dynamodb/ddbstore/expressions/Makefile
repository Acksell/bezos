PHONY: gen test

gen:
	pigeon -o writeconditions/parser/parser_gen.go writeconditions/parser/cond_expr.peg
	pigeon -o keyconditions/parser/parser_gen.go keyconditions/parser/key_cond_expr.peg

test:
	go test ./...
