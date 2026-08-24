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

# --- standardized check surface -------------------------------------------
# One target, the same set of checks, in every module. CI calls exactly this,
# so a check can no longer pass locally and be silently absent in CI (or the
# reverse). The lint/security tools are pinned by this module's go.mod tool directives.
#
# CHECK_GO_DIRS scopes gosec: gosec is NOT module-aware, so a bare ./... is a
# filesystem walk that descends into nested .worktrees/ checkouts, which are
# separate modules. go vet and staticcheck are module-aware and need no scope.
CHECK_GO_DIRS = $(shell GOWORK=off go list -f '{{.Dir}}' ./...)
# CHECK_GO_FILES is what gofmt gets. Never hand it CHECK_GO_DIRS: gofmt RECURSES
# into directory operands, so for a module with a root package it would walk the
# whole tree, nested .worktrees/ checkouts included.
CHECK_GO_FILES = $(foreach dir,$(CHECK_GO_DIRS),$(wildcard $(dir)/*.go))

check-staticcheck:
	GOWORK=off go tool staticcheck ./...

check-gosec:
	GOWORK=off go tool gosec -quiet $(CHECK_GO_DIRS)

check-vuln:
	GOWORK=off go mod verify
	GOWORK=off go tool govulncheck ./...

build:
	GOWORK=off go build ./...

check: fmt-check vet check-staticcheck check-gosec check-vuln test build

.PHONY: check check-staticcheck check-gosec check-vuln fmt fmt-check vet test build
