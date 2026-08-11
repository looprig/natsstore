package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const offlineExamplesCommand = "GOWORK=off go test -race ./..."

type examplesManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	Repository    string `json:"repository"`
	ProofSources  []struct {
		ID     string `json:"id"`
		Type   string `json:"type"`
		Path   string `json:"path"`
		Symbol string `json:"symbol,omitempty"`
	} `json:"proofSources"`
	Examples []struct {
		ID             string            `json:"id"`
		Ecosystem      string            `json:"ecosystem"`
		Owner          string            `json:"owner"`
		SourcePath     string            `json:"sourcePath"`
		Availability   string            `json:"availability"`
		Versions       map[string]string `json:"versions"`
		OfflineCommand string            `json:"offlineCommand"`
		Assertion      string            `json:"assertion"`
		WorkflowPath   string            `json:"workflowPath"`
		JobID          string            `json:"jobId"`
		Cleanup        string            `json:"cleanup"`
		LiveGate       json.RawMessage   `json:"liveGate"`
		ProofIDs       []string          `json:"proofIds"`
	} `json:"examples"`
}

func TestDocsExamplesArtifacts(t *testing.T) {
	repositoryRoot := filepath.Clean("..")
	manifestData, err := os.ReadFile(filepath.Join(repositoryRoot, "testdata/docs/examples.json"))
	if err != nil {
		t.Fatalf("read examples manifest: %v", err)
	}

	var manifest examplesManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode examples manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Repository != "natsstore" {
		t.Fatalf("manifest identity = schema %d repository %q", manifest.SchemaVersion, manifest.Repository)
	}

	expectedProofs := map[string]struct {
		proofType string
		path      string
		symbol    string
	}{
		"example-natsstore-embedded-composite-fixture": {
			proofType: "executable-fixture",
			path:      "examples/embedded/example_test.go",
			symbol:    "Example_embeddedComposite",
		},
		"example-natsstore-artifacts-contract-test": {
			proofType: "test",
			path:      "examples/contract_test.go",
			symbol:    "TestDocsExamplesArtifacts",
		},
		"example-natsstore-open-source": {
			proofType: "source",
			path:      "natsstore.go",
			symbol:    "Open",
		},
	}
	proofs := make(map[string]bool, len(manifest.ProofSources))
	for _, proof := range manifest.ProofSources {
		expected, ok := expectedProofs[proof.ID]
		if !ok {
			t.Errorf("unexpected proof source ID %q", proof.ID)
		} else if proof.Type != expected.proofType || proof.Path != expected.path || proof.Symbol != expected.symbol {
			t.Errorf("proof source %q = type %q path %q symbol %q, want type %q path %q symbol %q", proof.ID, proof.Type, proof.Path, proof.Symbol, expected.proofType, expected.path, expected.symbol)
		}
		if proofs[proof.ID] {
			t.Errorf("duplicate proof source ID %q", proof.ID)
		}
		proofs[proof.ID] = true
		if strings.Contains(proof.Path, "#") {
			t.Errorf("proof source %q path contains a symbol fragment: %q", proof.ID, proof.Path)
		}
		if _, err := os.Stat(filepath.Join(repositoryRoot, proof.Path)); err != nil {
			t.Errorf("proof source %q does not resolve locally: %v", proof.ID, err)
		}
	}
	if len(manifest.ProofSources) != len(expectedProofs) {
		t.Errorf("proof source count = %d, want %d", len(manifest.ProofSources), len(expectedProofs))
	}

	if len(manifest.Examples) != 1 {
		t.Fatalf("manifest examples = %d, want 1", len(manifest.Examples))
	}
	example := manifest.Examples[0]
	if example.ID != "example-natsstore-embedded-composite" {
		t.Errorf("example ID = %q", example.ID)
	}
	if example.Ecosystem != "go" || example.Owner != "natsstore" || example.Availability != "source-workspace" {
		t.Errorf("example classification is incorrect: %#v", example)
	}
	if len(example.Versions) != 1 || example.Versions["github.com/looprig/natsstore"] != "source-workspace" {
		t.Errorf("example versions = %#v", example.Versions)
	}
	if example.SourcePath != "examples/embedded/example_test.go" {
		t.Errorf("example sourcePath = %q", example.SourcePath)
	}
	if example.OfflineCommand != offlineExamplesCommand {
		t.Errorf("example offlineCommand = %q", example.OfflineCommand)
	}
	if example.Assertion != "Deterministic Go example output proves remote option validation without a dial, embedded Store ownership, composite ledger/KV/blob/lease behavior, typed conflicts, and idempotent Close." {
		t.Errorf("example assertion = %q", example.Assertion)
	}
	if example.WorkflowPath != ".github/workflows/docs-examples.yml" || example.JobID != "docs-examples" {
		t.Errorf("example workflow metadata = %q / %q", example.WorkflowPath, example.JobID)
	}
	if example.Cleanup != "The example closes its ledger cursor, blob reader, lease, and Store, then removes its unique temporary embedded StoreDir." {
		t.Errorf("example cleanup = %q", example.Cleanup)
	}
	if string(example.LiveGate) != "null" {
		t.Errorf("example liveGate = %s, want null because no remote service is contacted", example.LiveGate)
	}
	if len(example.ProofIDs) != 3 {
		t.Fatalf("example proofIds = %v, want fixture, contract-test, and source proofs", example.ProofIDs)
	}
	for _, proofID := range example.ProofIDs {
		if !proofs[proofID] {
			t.Errorf("example references unknown proof %q", proofID)
		}
	}

	workflow, err := os.ReadFile(filepath.Join(repositoryRoot, ".github/workflows/docs-examples.yml"))
	if err != nil {
		t.Fatalf("read docs examples workflow: %v", err)
	}
	for _, literal := range []string{
		"docs-examples:",
		offlineExamplesCommand,
		"GOWORK=off make check",
	} {
		if !strings.Contains(string(workflow), literal) {
			t.Errorf("workflow does not contain %q", literal)
		}
	}
}
