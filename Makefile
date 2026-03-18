install:
	go install ./dynamodb/cmd/ddb

PHONY: test
test:
	go test ./...
