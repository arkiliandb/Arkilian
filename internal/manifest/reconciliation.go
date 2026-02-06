package manifest

import (
	"context"
	"fmt"
	"time"

	"github.com/arkilian/arkilian/internal/storage"
)

// ReconciliationReport contains the results of a manifest-storage reconciliation.
type ReconciliationReport struct {
	// DanglingEntries are manifest records whose object_path does not exist in storage.
	DanglingEntries []DanglingEntry
	// OrphanedObjects are storage objects with no corresponding manifest record.
	OrphanedObjects []string
	// TotalManifestEntries is the number of active partitions checked.
	TotalManifestEntries int
	// TotalStorageObjects is the number of storage objects scanned.
	TotalStorageObjects int
	// RunAt is when the reconciliation was performed.
	RunAt time.Time
}

// DanglingEntry represents a manifest record pointing to a missing storage object.
type DanglingEntry struct {
	PartitionID string
	ObjectPath  string
}

// HasIssues returns true if the report contains any dangling entries or orphaned objects.
func (r *ReconciliationReport) HasIssues() bool {
	return len(r.DanglingEntries) > 0 || len(r.OrphanedObjects) > 0
}

// Reconcile checks consistency between the manifest catalog and object storage.
// It detects dangling manifest entries (manifest references non-existent objects)
// and orphaned storage objects (objects not tracked in the manifest).
func Reconcile(ctx context.Context, catalog Catalog, store storage.ObjectStorage, storagePrefix string) (*ReconciliationReport, error) {
	report := &ReconciliationReport{
		RunAt: time.Now(),
	}

	// Step 1: Get all active partitions from the manifest.
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: failed to list manifest partitions: %w", err)
	}
	report.TotalManifestEntries = len(partitions)

	// Build a set of known object paths from the manifest.
	manifestPaths := make(map[string]string) // object_path -> partition_id
	for _, p := range partitions {
		manifestPaths[p.ObjectPath] = p.PartitionID
	}

	// Step 2: Check each manifest entry for existence in storage (dangling detection).
	for _, p := range partitions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		exists, err := store.Exists(ctx, p.ObjectPath)
		if err != nil {
			return nil, fmt.Errorf("reconciliation: failed to check object %s: %w", p.ObjectPath, err)
		}
		if !exists {
			report.DanglingEntries = append(report.DanglingEntries, DanglingEntry{
				PartitionID: p.PartitionID,
				ObjectPath:  p.ObjectPath,
			})
		}
	}

	// Step 3: List all objects in storage and find orphans.
	objects, err := store.ListObjects(ctx, storagePrefix)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: failed to list storage objects: %w", err)
	}
	report.TotalStorageObjects = len(objects)

	for _, objPath := range objects {
		if _, tracked := manifestPaths[objPath]; !tracked {
			report.OrphanedObjects = append(report.OrphanedObjects, objPath)
		}
	}

	return report, nil
}
