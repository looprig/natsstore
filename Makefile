.PHONY: test test-integration fmt-check fmt vet check lint vuln secure fuzz

# Module's own package dirs, excluding vendor/ and any nested worktree modules.
GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' ./...)

test:
	GOWORK=off go test -race ./...

test-integration:
	GOWORK=off go test -tags integration -race ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

# Format the whole module in place.
fmt:
	GOWORK=off gofmt -w $(GO_DIRS)

vet:
	GOWORK=off go vet ./...

check: fmt-check vet test

lint: fmt-check vet
	GOWORK=off go tool staticcheck ./...
	# gosec is NOT module-aware: its ./... is a filesystem walk that descends into
	# nested checkouts (separate modules) and, under -mod=vendor, reports
	# modules.txt desyncs for those foreign trees. Scope it to THIS module's
	# package dirs via GO_DIRS (the same go-list idiom fmt/fmt-check use). go vet
	# and staticcheck are module-aware (go list stops at module boundaries), so
	# they need no scoping.
	GOWORK=off go tool gosec $(GO_DIRS)

vuln:
	GOWORK=off go mod verify
	GOWORK=off go tool govulncheck ./...

secure: lint vuln

fuzz:
	@echo "Usage: go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s"
