package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

var (
	ErrRolloutNotFound = errors.New("application rollout not found")
	ErrRolloutConflict = errors.New("application rollout generation conflict")
)

const rolloutBucketCount = 10000

type RolloutPolicy struct {
	TenantID         string    `json:"tenant_id"`
	AppCode          string    `json:"app_code"`
	Generation       uint64    `json:"generation"`
	StableVersion    uint64    `json:"stable_version"`
	CandidateVersion uint64    `json:"candidate_version"`
	BasisPoints      int       `json:"basis_points"`
	TestUserIDs      []string  `json:"test_user_ids"`
	Ingresses        []string  `json:"ingresses"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type RolloutUpdate struct {
	ExpectedGeneration uint64   `json:"expected_generation"`
	BasisPoints        int      `json:"basis_points"`
	TestUserIDs        []string `json:"test_user_ids"`
	Ingresses          []string `json:"ingresses"`
}

type ReleaseVariant string

const (
	ReleaseStable    ReleaseVariant = "stable"
	ReleaseCandidate ReleaseVariant = "candidate"
)

type ReleaseTarget struct {
	SessionKey     string
	PlatformUserID string
	Ingress        string
	Scope          string
}

type ReleaseSelection struct {
	Snapshot          Snapshot
	Variant           ReleaseVariant
	RolloutGeneration uint64
}

func ResolveRelease(ctx context.Context, repository Repository, active Snapshot, target ReleaseTarget) (ReleaseSelection, error) {
	if repository == nil {
		return ReleaseSelection{}, errors.New("tenant repository is required for release routing")
	}
	rollout, err := repository.GetRollout(ctx, active.Config.TenantID, active.Config.AppCode)
	if errors.Is(err, ErrRolloutNotFound) {
		return ReleaseSelection{Snapshot: active, Variant: ReleaseStable}, nil
	}
	if err != nil {
		return ReleaseSelection{}, fmt.Errorf("read application rollout: %w", err)
	}
	version, variant := SelectRelease(active, &rollout, target)
	if version == active.Config.ConfigVersion {
		return ReleaseSelection{Snapshot: active, Variant: variant, RolloutGeneration: rollout.Generation}, nil
	}
	candidate, err := repository.GetVersion(ctx, active.Config.TenantID, active.Config.AppCode, version)
	if err != nil {
		return ReleaseSelection{}, fmt.Errorf("resolve rollout candidate version %d: %w", version, err)
	}
	if candidate.Config.Status != config.AgentActive {
		return ReleaseSelection{}, errors.New("rollout candidate is not active")
	}
	return ReleaseSelection{Snapshot: candidate, Variant: variant, RolloutGeneration: rollout.Generation}, nil
}

func normalizeRolloutUpdate(update RolloutUpdate) (RolloutUpdate, error) {
	if update.BasisPoints < 0 || update.BasisPoints > rolloutBucketCount {
		return RolloutUpdate{}, fmt.Errorf("rollout basis_points must be between 0 and %d", rolloutBucketCount)
	}
	update.TestUserIDs = normalizedUniqueStrings(update.TestUserIDs)
	update.Ingresses = normalizedUniqueStrings(update.Ingresses)
	return update, nil
}

func normalizedUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Strings(normalized)
	return normalized
}

// SelectRelease applies one rollout policy after canonical Session resolution.
// Generation is deliberately not part of the hash salt, so changing traffic
// percentage does not reshuffle Sessions that were already bucketed.
func SelectRelease(active Snapshot, rollout *RolloutPolicy, target ReleaseTarget) (uint64, ReleaseVariant) {
	if rollout == nil || rollout.StableVersion != active.Config.ConfigVersion || rollout.CandidateVersion == 0 {
		return active.Config.ConfigVersion, ReleaseStable
	}
	ingress := strings.TrimSpace(target.Ingress)
	if len(rollout.Ingresses) > 0 {
		if !containsString(rollout.Ingresses, ingress) {
			return rollout.StableVersion, ReleaseStable
		}
	}
	platformUserID := strings.TrimSpace(target.PlatformUserID)
	if target.Scope != "group" && platformUserID != "" && containsString(rollout.TestUserIDs, platformUserID) {
		return rollout.CandidateVersion, ReleaseCandidate
	}
	if rollout.BasisPoints <= 0 {
		return rollout.StableVersion, ReleaseStable
	}
	if rollout.BasisPoints >= rolloutBucketCount {
		return rollout.CandidateVersion, ReleaseCandidate
	}
	identity := strings.TrimSpace(target.SessionKey)
	if identity == "" {
		return rollout.StableVersion, ReleaseStable
	}
	salt := fmt.Sprintf("%s/%s:%d:%d:%s", rollout.TenantID, rollout.AppCode, rollout.StableVersion, rollout.CandidateVersion, identity)
	digest := sha256.Sum256([]byte(salt))
	bucket := int(binary.BigEndian.Uint64(digest[:8]) % rolloutBucketCount)
	if bucket < rollout.BasisPoints {
		return rollout.CandidateVersion, ReleaseCandidate
	}
	return rollout.StableVersion, ReleaseStable
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func ReleaseIngress(channel, bindingID string) string {
	channel = strings.ToLower(strings.TrimSpace(channel))
	if channel == "web" {
		return "web"
	}
	bindingID = strings.TrimSpace(bindingID)
	if channel == "" || bindingID == "" {
		return ""
	}
	return channel + "/" + bindingID
}

func validateRolloutIngresses(stable config.TenantConfig, ingresses []string) error {
	allowed := map[string]struct{}{"web": {}}
	for _, binding := range stable.Channels {
		ingress := ReleaseIngress(binding.Type, binding.BindingID)
		if ingress != "" {
			allowed[ingress] = struct{}{}
		}
	}
	for _, ingress := range ingresses {
		if _, ok := allowed[ingress]; !ok {
			return fmt.Errorf("rollout ingress %q is not owned by the stable application", ingress)
		}
	}
	return nil
}

func sameChannelTopology(stable, candidate config.TenantConfig) bool {
	return channelTopologySignature(stable.Channels) == channelTopologySignature(candidate.Channels)
}

func channelTopologySignature(bindings []config.ChannelBinding) string {
	parts := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		allowlist := append([]string(nil), binding.Allowlist...)
		sort.Strings(allowlist)
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(binding.Type), strings.TrimSpace(binding.BindingID),
			strings.TrimSpace(binding.CredentialRef), strings.TrimSpace(binding.TrustedEnterpriseID), binding.EffectiveAccessPolicy(), strings.Join(allowlist, ","),
		}, "\x00"))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x01")
}
