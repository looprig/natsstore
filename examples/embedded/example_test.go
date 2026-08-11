package embedded_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/looprig/natsstore"
	"github.com/looprig/storage"
)

func Example_embeddedComposite() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Invalid remote/local combinations fail during option validation, before a
	// network dial or filesystem change. A real remote deployment would pass only URL.
	_, err := natsstore.Open(ctx, natsstore.Options{
		URL:         "nats://broker.example:4222",
		EmbeddedDir: "/var/lib/example",
	})
	var optionsErr *natsstore.OptionsError
	fmt.Println("remote options", errors.As(err, &optionsErr), optionsErr.Field)

	temporaryParent, err := os.MkdirTemp("", "natsstore-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temporaryParent)

	store, err := natsstore.Open(ctx, natsstore.Options{
		EmbeddedDir: filepath.Join(temporaryParent, "jetstream"),
	})
	if err != nil {
		panic(err)
	}
	defer store.Close(context.Background())
	backend := store.Backend()
	fmt.Println("composite", backend == store.Composite)

	const ledgerName = "sessions/example"
	if err := backend.Ledger.Append(ctx, ledgerName, 0, []byte("created")); err != nil {
		panic(err)
	}
	err = backend.Ledger.Append(ctx, ledgerName, 0, []byte("duplicate"))
	var ledgerConflict *storage.ConflictError
	fmt.Println("ledger conflict", errors.As(err, &ledgerConflict))
	cursor, err := backend.Ledger.Read(ctx, ledgerName, 1)
	if err != nil {
		panic(err)
	}
	record, err := cursor.Next(ctx)
	if err != nil {
		panic(err)
	}
	if err := cursor.Close(); err != nil {
		panic(err)
	}
	fmt.Println("ledger", record.Seq, string(record.Payload))

	const metadataKey = "catalog/example"
	revision, err := backend.KV.Put(ctx, metadataKey, 0, []byte("ready"))
	if err != nil {
		panic(err)
	}
	metadata, persistedRevision, err := backend.KV.Get(ctx, metadataKey)
	if err != nil {
		panic(err)
	}
	fmt.Println("kv", revision, persistedRevision, string(metadata))

	const blobKey = "snapshots/example"
	if err := backend.Blobs.Put(ctx, blobKey, bytes.NewBufferString("snapshot")); err != nil {
		panic(err)
	}
	err = backend.Blobs.Put(ctx, blobKey, bytes.NewBufferString("different"))
	var blobConflict *storage.BlobConflictError
	fmt.Println("blob conflict", errors.As(err, &blobConflict))
	reader, err := backend.Blobs.Get(ctx, blobKey)
	if err != nil {
		panic(err)
	}
	blob, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	if err := reader.Close(); err != nil {
		panic(err)
	}
	fmt.Println("blob", string(blob))

	lease, err := backend.Leaser.Acquire(ctx, "locks/example")
	if err != nil {
		panic(err)
	}
	_, err = backend.Leaser.Acquire(ctx, "locks/example")
	var leaseHeld *storage.LeaseHeldError
	fmt.Println("lease", lease.Epoch(), errors.As(err, &leaseHeld))
	if err := lease.Release(ctx); err != nil {
		panic(err)
	}

	firstClose := store.Close(ctx)
	secondClose := store.Close(ctx)
	fmt.Println("close", firstClose, secondClose)

	// Output:
	// remote options true URL/EmbeddedDir
	// composite true
	// ledger conflict true
	// ledger 1 created
	// kv 1 1 ready
	// blob conflict true
	// blob snapshot
	// lease 1 true
	// close <nil> <nil>
}
