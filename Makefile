.PHONY: test test-integration fmt-check vet check

test:
	GOWORK=off go test -race ./...

test-integration:
	GOWORK=off go test -tags integration -race ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	GOWORK=off go vet ./...

check: fmt-check vet test
