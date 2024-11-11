PHONY: gen test

gen:
	pigeon -o writeconditions/parser_gen.go writeconditions/cond_expr.peg
	pigeon -o keyconditions/parser_gen.go keyconditions/key_cond_expr.peg

test:
	go test ./...
