PHONY: gen test

install:
	go install github.com/mna/pigeon@latest

gen:
	pigeon -o writeconditions/parser/parser_gen.go writeconditions/parser/cond_expr.peg
	pigeon -o keyconditions/parser/parser_gen.go keyconditions/parser/key_cond_expr.peg
	pigeon -o updateexpressions/parser/parser_gen.go updateexpressions/parser/update_expr.peg
	pigeon -o projectionexpressions/parser/parser_gen.go projectionexpressions/parser/projection_expr.peg

test:
	go test ./...
