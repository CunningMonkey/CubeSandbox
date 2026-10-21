// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
)

// templateRootfsGenerationLimit bounds how many new snapshot names a retry
// may publish before giving up. The canonical name is generation 1 and is
// never renamed or deleted.
const templateRootfsGenerationLimit = 32

type templateRootfsCommitter interface {
	createTemplateSnapshotPath(sourceName, snapshotName string) (string, error)
	ensureSnapshotOrigin(sourceName, snapshotName string) error
	ResolveDevPath(ctx context.Context, name, kind string) (string, error)
	GetSizeBytes(ctx context.Context, name string) (uint64, error)
}

// commitTemplateRootfsRetryable publishes templateID's rootfs snapshot.
// A complete snapshot of the same origin is reused. A partial, missing, or
// different-origin object is left in place and the next generation name is
// published instead. Nothing is deleted.
func commitTemplateRootfsRetryable(ctx context.Context, backend templateRootfsCommitter, sourceName, templateID string) (*cowVolume, error) {
	canonical := cowTemplateRootfsName(templateID)
	var lastErr error
	for gen := 1; gen <= templateRootfsGenerationLimit; gen++ {
		name := canonical
		if gen > 1 {
			name = fmt.Sprintf("%s-gen%d", canonical, gen)
		}
		devPath, err := backend.createTemplateSnapshotPath(sourceName, name)
		if err == nil {
			return newCowVolume(name, cowKindSnapshot, 0, devPath), nil
		}
		lastErr = err
		if !errors.Is(err, ErrCowObjectAlreadyExists) {
			return nil, err
		}
		if originErr := backend.ensureSnapshotOrigin(sourceName, name); originErr != nil {
			continue
		}
		size, sizeErr := backend.GetSizeBytes(ctx, name)
		if sizeErr != nil || size == 0 {
			continue
		}
		resolved, resolveErr := backend.ResolveDevPath(ctx, name, cowKindSnapshot)
		if resolveErr != nil {
			return nil, resolveErr
		}
		return newCowVolume(name, cowKindSnapshot, 0, resolved), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("exhausted %d generations", templateRootfsGenerationLimit)
	}
	return nil, fmt.Errorf("template rootfs %s could not be reused or republished: %w", canonical, lastErr)
}
