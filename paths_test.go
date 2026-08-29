package natsstore

import (
	"reflect"
	"testing"

	"github.com/looprig/storage"
)

var (
	_ storage.PathReporter = (*Store)(nil)
	_ storage.PathReporter = (*ledgerStore)(nil)
	_ storage.PathReporter = (*leaserStore)(nil)
	_ storage.PathReporter = (*kvStore)(nil)
	_ storage.PathReporter = (*blobStore)(nil)
	_ storage.PathReporter = (*orderedStore)(nil)
)

func TestLocalPathReporter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		paths []string
		want  []string
	}{
		{name: "empty"},
		{name: "one path", paths: []string{"/data/jetstream"}, want: []string{"/data/jetstream"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := append([]string(nil), tt.paths...)
			reporter := newLocalPathReporter(input...)
			if len(input) > 0 {
				input[0] = "/mutated-input"
			}

			got := reporter.StoragePaths()
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("StoragePaths() = %v, want %v", got, tt.want)
			}
			if len(got) > 0 {
				got[0] = "/mutated-output"
			}
			if again := reporter.StoragePaths(); !reflect.DeepEqual(again, tt.want) {
				t.Errorf("StoragePaths() after caller mutation = %v, want %v", again, tt.want)
			}
		})
	}
}

func TestStoreStoragePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reporter localPathReporter
		want     []string
	}{
		{name: "remote has no local paths", reporter: newLocalPathReporter()},
		{name: "embedded path", reporter: newLocalPathReporter("/data/jetstream"), want: []string{"/data/jetstream"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{pathReporter: tt.reporter}
			if got := store.StoragePaths(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StoragePaths() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorePathReporterMethodSet(t *testing.T) {
	t.Parallel()

	reporterType := reflect.TypeOf((*storage.PathReporter)(nil)).Elem()
	tests := []struct {
		name   string
		typeOf reflect.Type
		want   bool
	}{
		{name: "pointer implements", typeOf: reflect.TypeOf((*Store)(nil)), want: true},
		{name: "value does not implement", typeOf: reflect.TypeOf(Store{}), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.typeOf.Implements(reporterType); got != tt.want {
				t.Errorf("Implements(storage.PathReporter) = %v, want %v", got, tt.want)
			}
		})
	}
}
