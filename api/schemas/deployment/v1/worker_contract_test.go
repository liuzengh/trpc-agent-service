package deploymentv1

import (
	"errors"
	"strings"
	"testing"
)

// A release-pin mismatch is generation skew, not a capability refusal: the same
// manifest is executable by a consumer that runs the producer's release. It must
// stay distinguishable so a release-pinned consumer can wait for that release
// instead of terminating the work permanently.
func TestWorkerV1ContractDigestMismatchIsReleaseSkew(t *testing.T) {
	c := workerFixture(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	err := ValidateWorkerV1(c, "sha256:"+strings.Repeat("0", 64))
	if !errors.Is(err, ErrWorkerV1ContractMismatch) {
		t.Fatalf("release pin mismatch = %v", err)
	}
	if errors.Is(err, ErrUnsupportedWorkerManifest) {
		t.Fatal("release pin mismatch was reported as an unsupported manifest")
	}
}

// A different manifest format or platform version stays a capability refusal:
// those manifests are not merely pinned to another release.
func TestWorkerV1FormatMismatchStaysUnsupported(t *testing.T) {
	c := workerFixture(t)
	c.PlatformContract.Version = "platform-v0"
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); !errors.Is(err, ErrUnsupportedWorkerManifest) || errors.Is(err, ErrWorkerV1ContractMismatch) {
		t.Fatalf("platform version mismatch = %v", err)
	}
}
