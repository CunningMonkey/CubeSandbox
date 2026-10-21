// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/pkgs/blobstore"
	"github.com/tencentcloud/CubeSandbox/pkgs/blobstore/configenv"
	"github.com/tencentcloud/CubeSandbox/pkgs/blobstore/gateway"
	"github.com/tencentcloud/CubeSandbox/pkgs/blobstore/signer"
)

const (
	// EnvArtifactDownloadTTL is the lifetime of a Cubelet-facing download grant.
	// Examples: "1h" (default), "2h". Values above seven days are rejected
	// because that is the SigV4 maximum.
	EnvArtifactDownloadTTL = "CUBE_ARTIFACT_DOWNLOAD_TTL"

	defaultArtifactDownloadTTL = time.Hour
	maxArtifactDownloadTTL     = 7 * 24 * time.Hour

	// ArtifactObjectMountPath is the CubeTemplateCenter route that streams an
	// fs-backed ext4 after checking the HMAC grant. It is not the shared
	// build/delete API and it does not accept the callback token.
	ArtifactObjectMountPath = "/internal/artifact/object"
)

// artifactDownloadTTL is the grant lifetime used for both S3 presigns and
// TC HMAC URLs. Unset means one hour. Unparseable, non-positive, or over-long
// values fall back to one hour so a bad setting cannot restore a seven-day URL.
func artifactDownloadTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv(EnvArtifactDownloadTTL))
	if raw == "" {
		return defaultArtifactDownloadTTL
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 || d > maxArtifactDownloadTTL {
		log.G(context.Background()).Warnf("%s=%q is not a duration in (0, %s]; using %s", EnvArtifactDownloadTTL, raw, maxArtifactDownloadTTL, defaultArtifactDownloadTTL)
		return defaultArtifactDownloadTTL
	}
	return d
}

// ArtifactDownloadTTL reports the current grant lifetime.
func ArtifactDownloadTTL() time.Duration {
	return artifactDownloadTTL()
}

// issueCubeletArtifactURL is the seam tests replace. Production mints a
// direct S3 or TC URL and never a CubeMaster byte proxy.
var issueCubeletArtifactURL = directCubeletArtifactURL

// IssueDirectArtifactDownload loads the artifact row and mints a fresh
// Cubelet-facing URL. Used by the authorize endpoint after a grant expires.
func IssueDirectArtifactDownload(ctx context.Context, artifactID string) (string, error) {
	artifactID = strings.TrimSpace(artifactID)
	if artifactID == "" {
		return "", fmt.Errorf("artifact_id is required")
	}
	artifact, err := getRootfsArtifactByID(ctx, artifactID)
	if err != nil {
		return "", err
	}
	return issueCubeletArtifactURL(ctx, artifact)
}

func directCubeletArtifactURL(ctx context.Context, artifact *models.RootfsArtifact) (string, error) {
	if artifact == nil || strings.TrimSpace(artifact.ArtifactID) == "" {
		return "", fmt.Errorf("artifact id is required")
	}
	rememberArtifactLocator(ctx, artifact)
	if artifactUsesS3(artifact) {
		fresh, err := presignArtifactGetURL(ctx, artifact)
		if err == nil && directDownloadURL(fresh) {
			return fresh, nil
		}
		if err != nil && err != errS3PresignNotConfigured {
			log.G(ctx).Warnf("s3 presign for artifact %s failed, asking template center: %v", artifact.ArtifactID, err)
		}
	}
	return requestTemplateCenterDownloadGrant(ctx, artifact.ArtifactID, artifactDownloadTTL())
}

func directDownloadURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || blobstore.IsObjectLocator(raw) {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	if strings.Contains(u.Path, "/cube/template/") && strings.Contains(u.Path, "download") {
		return false
	}
	return true
}

func artifactUsesS3(artifact *models.RootfsArtifact) bool {
	if artifact == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(artifact.StorageBackend)) {
	case "s3":
		return true
	case "fs":
		return false
	}
	stored := strings.TrimSpace(artifact.ArtifactURL)
	if stored == "" {
		return false
	}
	if blobstore.IsObjectLocator(stored) {
		return strings.Contains(strings.ToLower(stored), ":s3:")
	}
	u, err := url.Parse(stored)
	if err != nil {
		return false
	}
	return u.Query().Has("X-Amz-Signature") || u.Query().Has("X-Amz-Algorithm")
}

func rememberArtifactLocator(ctx context.Context, artifact *models.RootfsArtifact) {
	if artifact == nil || !isReady() {
		return
	}
	backend := strings.TrimSpace(artifact.StorageBackend)
	key := strings.TrimSpace(artifact.ObjectKey)
	if backend != "" && key != "" {
		return
	}
	if backend == "" {
		if artifactUsesS3(artifact) {
			backend = "s3"
		} else {
			backend = "fs"
		}
	}
	if key == "" {
		key = blobstore.ArtifactExt4Key(configenv.EnvOr(configenv.EnvS3ArtifactPrefix), artifact.ArtifactID)
	}
	updates := map[string]any{}
	if strings.TrimSpace(artifact.StorageBackend) == "" {
		updates["storage_backend"] = backend
		artifact.StorageBackend = backend
	}
	if strings.TrimSpace(artifact.ObjectKey) == "" {
		updates["object_key"] = key
		artifact.ObjectKey = key
	}
	if len(updates) == 0 {
		return
	}
	if err := store.db.WithContext(ctx).Model(&models.RootfsArtifact{}).
		Where("artifact_id = ?", artifact.ArtifactID).
		Updates(updates).Error; err != nil {
		log.G(ctx).Warnf("persist artifact locator %s: %v", artifact.ArtifactID, err)
	}
}

type downloadGrantResponse struct {
	URL       string `json:"url"`
	ExpiresIn int64  `json:"expires_in"`
}

var requestTemplateCenterDownloadGrant = func(ctx context.Context, artifactID string, ttl time.Duration) (string, error) {
	base := templateCenterBaseURL()
	if base == "" {
		return "", fmt.Errorf("cannot mint a template-center download url for %s: %s is empty", artifactID, config.EnvTemplateCenterAddr)
	}
	if ttl <= 0 {
		ttl = artifactDownloadTTL()
	}
	payload, err := json.Marshal(map[string]any{
		"artifact_id": artifactID,
		"public_base": base,
		"ttl_seconds": int64(ttl.Seconds()),
	})
	if err != nil {
		return "", err
	}
	endpoint := strings.TrimRight(base, "/") + "/tc/api/v1/artifact/download-grant"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(os.Getenv(constants.TemplateCallbackTokenEnv)); token != "" {
		req.Header.Set(constants.TemplateCallbackTokenHeader, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("template center download grant: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("template center download grant status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out downloadGrantResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("template center download grant: %w", err)
	}
	if !directDownloadURL(out.URL) {
		return "", fmt.Errorf("template center returned a non-direct download url for %s", artifactID)
	}
	return out.URL, nil
}

func templateCenterBaseURL() string {
	if cfg := config.GetConfig(); cfg != nil {
		if addr := strings.TrimSpace(cfg.TemplateCenterAddr()); addr != "" {
			return strings.TrimRight(addr, "/")
		}
	}
	return strings.TrimRight(strings.TrimSpace(os.Getenv(config.EnvTemplateCenterAddr)), "/")
}

// SignStoredArtifactURL mints a direct GET for artifactID. S3/MinIO gets a
// short presign. fs (local disk, CBS, CFS) gets an HMAC URL on publicBase
// that CubeTemplateCenter's object gateway can verify. The callback token
// is not part of the URL.
func SignStoredArtifactURL(ctx context.Context, artifactID, publicBase string, ttl time.Duration) (string, error) {
	artifact, err := getRootfsArtifactByID(ctx, artifactID)
	if err != nil {
		return "", err
	}
	if ttl <= 0 || ttl > maxArtifactDownloadTTL {
		ttl = artifactDownloadTTL()
	}
	st := sharedArtifactStore()
	if st == nil {
		return "", errS3PresignNotConfigured
	}
	key := artifactStoreKey(ctx, artifact)
	if artifactUsesS3(artifact) || st.backend == "s3" {
		u, err := st.store.SignedGetURL(ctx, key, ttl)
		if err != nil {
			return "", err
		}
		if !directDownloadURL(u) {
			return "", fmt.Errorf("object store did not return a direct url for %s", artifactID)
		}
		return u, nil
	}
	type hasSigner interface {
		Signer() *signer.Signer
	}
	hs, ok := st.store.(hasSigner)
	if !ok || hs.Signer() == nil || len(hs.Signer().Key) == 0 {
		return "", fmt.Errorf("fs download signer is not ready")
	}
	publicBase = strings.TrimSpace(publicBase)
	if publicBase == "" {
		publicBase = templateCenterBaseURL()
	}
	return hs.Signer().URL(publicBase, ArtifactObjectMountPath, key, ttl)
}

// ArtifactObjectHandler serves HMAC-signed fs object GETs. Nil store yields
// a handler that answers 503.
func ArtifactObjectHandler() http.Handler {
	st := sharedArtifactStore()
	if st == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "object store unavailable", http.StatusServiceUnavailable)
		})
	}
	type hasSigner interface {
		Signer() *signer.Signer
	}
	hs, ok := st.store.(hasSigner)
	if !ok || hs.Signer() == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "object signer unavailable", http.StatusServiceUnavailable)
		})
	}
	return &gateway.Handler{Store: st.store, Signer: hs.Signer()}
}
