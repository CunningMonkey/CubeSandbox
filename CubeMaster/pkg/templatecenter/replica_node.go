// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"context"
	"strings"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
)

type replicaNodeKind int

const (
	// replicaNodeUnknown means the node cache has not been loaded. Callers
	// keep the stored address and the historical READY verdict so a cold
	// start does not black-hole every replica.
	replicaNodeUnknown replicaNodeKind = iota
	// replicaNodeLive is present in the cache and healthy. Dial its current IP.
	replicaNodeLive
	// replicaNodeUnreachable is present but unhealthy. It is not schedulable
	// and a failed cleanup must be retried later.
	replicaNodeUnreachable
	// replicaNodeRetired is absent from a populated cache. Nothing is left to dial.
	replicaNodeRetired
)

func classifyReplicaNode(nodeID, nodeIP string) (string, replicaNodeKind) {
	nodeID = strings.TrimSpace(nodeID)
	nodeIP = strings.TrimSpace(nodeIP)
	if len(localcache.GetCacheItems()) == 0 {
		return nodeIP, replicaNodeUnknown
	}
	var foundIP string
	var healthy bool
	var found bool
	if nodeID != "" {
		if n, ok := localcache.GetNode(nodeID); ok && n != nil {
			found = true
			healthy = n.Healthy
			foundIP = strings.TrimSpace(n.HostIP())
		}
	}
	if !found && nodeIP != "" {
		if n, ok := localcache.GetNodesByIp(nodeIP); ok && n != nil {
			found = true
			healthy = n.Healthy
			foundIP = strings.TrimSpace(n.HostIP())
		}
	}
	if !found {
		return "", replicaNodeRetired
	}
	if foundIP == "" {
		foundIP = nodeIP
	}
	if !healthy {
		return foundIP, replicaNodeUnreachable
	}
	return foundIP, replicaNodeLive
}

func replicaNodeSchedulable(nodeID, nodeIP string) bool {
	_, kind := classifyReplicaNode(nodeID, nodeIP)
	return kind == replicaNodeLive || kind == replicaNodeUnknown
}

// applyReplicaNodeTruth adjusts a READY replica for callers that surface
// status. A retired node becomes FAILED. A temporarily unreachable node stays
// READY in the stored string; scheduling excludes it separately. A live node
// reports its current address.
func applyReplicaNodeTruth(status *ReplicaStatus) {
	if status == nil || strings.TrimSpace(status.Status) != ReplicaStatusReady {
		return
	}
	ip, kind := classifyReplicaNode(status.NodeID, status.NodeIP)
	switch kind {
	case replicaNodeLive:
		if ip != "" {
			status.NodeIP = ip
		}
	case replicaNodeRetired:
		status.Status = ReplicaStatusFailed
		if strings.TrimSpace(status.ErrorMessage) == "" {
			status.ErrorMessage = "node retired"
		}
	}
}

func markReplicaCleanupRequired(ctx context.Context, templateID, nodeID, nodeIP string) {
	if !isReady() {
		return
	}
	templateID = strings.TrimSpace(templateID)
	nodeID = strings.TrimSpace(nodeID)
	nodeIP = strings.TrimSpace(nodeIP)
	if templateID == "" || (nodeID == "" && nodeIP == "") {
		return
	}
	q := store.db.WithContext(ctx).Table(constants.TemplateReplicaTableName).
		Where("template_id = ?", templateID)
	if nodeID != "" {
		q = q.Where("node_id = ?", nodeID)
	} else {
		q = q.Where("node_ip = ?", nodeIP)
	}
	if err := q.Update("cleanup_required", true).Error; err != nil {
		log.G(ctx).Warnf("mark replica cleanup required template=%s node=%s: %v", templateID, nodeID, err)
	}
}

func persistReplicaNodeIP(ctx context.Context, templateID, nodeID, nodeIP string) {
	if !isReady() {
		return
	}
	templateID = strings.TrimSpace(templateID)
	nodeID = strings.TrimSpace(nodeID)
	nodeIP = strings.TrimSpace(nodeIP)
	if templateID == "" || nodeID == "" || nodeIP == "" {
		return
	}
	if err := store.db.WithContext(ctx).Table(constants.TemplateReplicaTableName).
		Where("template_id = ? AND node_id = ?", templateID, nodeID).
		Update("node_ip", nodeIP).Error; err != nil {
		log.G(ctx).Warnf("persist replica address template=%s node=%s: %v", templateID, nodeID, err)
	}
}
