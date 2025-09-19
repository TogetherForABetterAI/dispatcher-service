# Makefile for data-dispatcher-service tests

.PHONY: test test-unit test-integration test-mnist test-coverage clean help

test:
	go test ./src/test/... -v

test-unit:
	go test ./src/test/unit/... -v

test-integration:
	go test ./src/test/integration/... -v

test-mnist:
	go test ./src/test/unit/... -v -run TestMNISTDataGeneration

test-coverage:
	go test ./src/test/... -v -coverprofile=coverage.out -coverpkg=./src/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

coverage-summary:
	@echo "=== Coverage Summary ==="
	go tool cover -func=coverage.out | grep -E "(total|client_processor|config)" || echo "Run 'make test-coverage' first"

clean:
	rm -f coverage.out coverage.html

deps:
	go mod tidy
	go get github.com/stretchr/testify@latest
