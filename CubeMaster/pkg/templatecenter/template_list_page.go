// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"context"
	"fmt"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
)

// DefaultTemplateListLimit is the page size used when a caller asks for a
// page but does not set a positive limit. The unpaged API (limit and offset
// both zero) still returns the full cached list.
const DefaultTemplateListLimit = 50

type templateListKey struct {
	TemplateID string `gorm:"column:template_id"`
	SortGroup  int    `gorm:"column:sort_group"`
}

// listTemplatesPage reads one ordered page from the database. Definitions
// come first (updated_at desc, template_id asc), then CREATE/REDO jobs that
// have no definition (template_id asc). Pause snapshots and DELETED/DELETING
// snapshot ids are omitted. The page is not written back as the full list cache.
func listTemplatesPage(ctx context.Context, opts TemplateListOptions) (*TemplateListResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = DefaultTemplateListLimit
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	total, err := countListedTemplates(ctx)
	if err != nil {
		return nil, err
	}
	if opts.Offset >= total || total == 0 {
		return &TemplateListResult{Templates: []TemplateInfo{}, Total: total}, nil
	}
	keys, err := queryTemplateListPage(ctx, opts)
	if err != nil {
		return nil, err
	}
	infos, err := loadTemplateListPage(ctx, keys)
	if err != nil {
		return nil, err
	}
	return &TemplateListResult{Templates: infos, Total: total}, nil
}

func countListedTemplates(ctx context.Context) (int, error) {
	var defCount int64
	if err := store.db.WithContext(ctx).Raw(definitionListCountSQL(), pausesnap.KindPauseSnapshot, StatusDeleted, StatusDeleting).Scan(&defCount).Error; err != nil {
		return 0, fmt.Errorf("count template definitions: %w", err)
	}
	var jobCount int64
	if err := store.db.WithContext(ctx).Raw(jobOnlyListCountSQL(), JobOperationCreate, JobOperationRedo, StatusDeleted, StatusDeleting).Scan(&jobCount).Error; err != nil {
		return 0, fmt.Errorf("count job-only templates: %w", err)
	}
	return int(defCount + jobCount), nil
}

func queryTemplateListPage(ctx context.Context, opts TemplateListOptions) ([]templateListKey, error) {
	var keys []templateListKey
	err := store.db.WithContext(ctx).Raw(
		templateListPageSQL(),
		pausesnap.KindPauseSnapshot, StatusDeleted, StatusDeleting,
		JobOperationCreate, JobOperationRedo, StatusDeleted, StatusDeleting,
		opts.Limit, opts.Offset,
	).Scan(&keys).Error
	if err != nil {
		return nil, fmt.Errorf("page template list: %w", err)
	}
	return keys, nil
}

func loadTemplateListPage(ctx context.Context, keys []templateListKey) ([]TemplateInfo, error) {
	if len(keys) == 0 {
		return []TemplateInfo{}, nil
	}
	ids := make([]string, 0, len(keys))
	defIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		ids = append(ids, key.TemplateID)
		if key.SortGroup == 0 {
			defIDs = append(defIDs, key.TemplateID)
		}
	}
	defsByID := map[string]models.TemplateDefinition{}
	if len(defIDs) > 0 {
		var defs []models.TemplateDefinition
		if err := store.db.WithContext(ctx).Table(constants.TemplateDefinitionTableName).
			Where("template_id IN ?", defIDs).Find(&defs).Error; err != nil {
			return nil, err
		}
		for _, def := range defs {
			defsByID[def.TemplateID] = def
		}
	}
	latestJob, err := latestCreateRedoJobs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]TemplateInfo, 0, len(keys))
	for _, key := range keys {
		if def, ok := defsByID[key.TemplateID]; ok {
			info := templateInfoFromDefinition(def)
			info.CreatedAt = formatUTCRFC3339(def.CreatedAt)
			info.ImageInfo = extractImageInfoFromRequestJSON(def.RequestJSON)
			if job := latestJob[key.TemplateID]; job != nil {
				info.ImageInfo = composeImageInfo(job.SourceImageRef, job.SourceImageDigest)
				info.JobID = latestJobIDFromJob(job)
			}
			out = append(out, info)
			continue
		}
		if job := latestJob[key.TemplateID]; job != nil {
			out = append(out, templateInfoFromJob(job))
		}
	}
	return out, nil
}

func latestCreateRedoJobs(ctx context.Context, templateIDs []string) (map[string]*models.TemplateImageJob, error) {
	out := map[string]*models.TemplateImageJob{}
	if len(templateIDs) == 0 {
		return out, nil
	}
	var jobs []models.TemplateImageJob
	if err := store.db.WithContext(ctx).Table(constants.TemplateImageJobTableName).
		Where("template_id IN ? AND operation IN ?", templateIDs, createRedoJobOperations).
		Order("attempt_no desc, id desc").
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	for i := range jobs {
		job := &jobs[i]
		if _, exists := out[job.TemplateID]; exists {
			continue
		}
		out[job.TemplateID] = job
	}
	return out, nil
}

func hiddenSnapshotPredicate(snapshotIDExpr string) string {
	return `NOT EXISTS (
		SELECT 1 FROM ` + constants.SnapshotTableName + ` s
		WHERE s.deleted_at IS NULL
		  AND s.snapshot_id = ` + snapshotIDExpr + `
		  AND UPPER(COALESCE(s.status, '')) IN (?, ?)
	)`
}

func definitionListCountSQL() string {
	return `SELECT COUNT(*) FROM ` + constants.TemplateDefinitionTableName + ` d
		WHERE d.deleted_at IS NULL
		  AND LOWER(COALESCE(d.kind, '')) != ?
		  AND ` + hiddenSnapshotPredicate("d.template_id")
}

func jobOnlyListCountSQL() string {
	return `SELECT COUNT(DISTINCT j.template_id) FROM ` + constants.TemplateImageJobTableName + ` j
		WHERE j.deleted_at IS NULL
		  AND j.operation IN (?, ?)
		  AND NOT EXISTS (
			SELECT 1 FROM ` + constants.TemplateDefinitionTableName + ` d
			WHERE d.deleted_at IS NULL AND d.template_id = j.template_id
		  )
		  AND ` + hiddenSnapshotPredicate("j.template_id")
}

func templateListPageSQL() string {
	return `SELECT template_id, sort_group FROM (
		SELECT d.template_id AS template_id, 0 AS sort_group, d.updated_at AS sort_time
		FROM ` + constants.TemplateDefinitionTableName + ` d
		WHERE d.deleted_at IS NULL
		  AND LOWER(COALESCE(d.kind, '')) != ?
		  AND ` + hiddenSnapshotPredicate("d.template_id") + `
		UNION ALL
		SELECT DISTINCT j.template_id AS template_id, 1 AS sort_group, NULL AS sort_time
		FROM ` + constants.TemplateImageJobTableName + ` j
		WHERE j.deleted_at IS NULL
		  AND j.operation IN (?, ?)
		  AND NOT EXISTS (
			SELECT 1 FROM ` + constants.TemplateDefinitionTableName + ` d
			WHERE d.deleted_at IS NULL AND d.template_id = j.template_id
		  )
		  AND ` + hiddenSnapshotPredicate("j.template_id") + `
	) listed
	ORDER BY sort_group ASC, sort_time DESC, template_id ASC
	LIMIT ? OFFSET ?`
}
