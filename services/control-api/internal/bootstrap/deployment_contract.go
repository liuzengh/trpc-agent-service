package bootstrap

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

var deploymentContractDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// DeploymentContractDigestFromEnvironment computes the effective frozen platform
// contract using endpoint hosts and optional pinned backend catalog digests. It does not load
// database, credential-key, or expected-digest configuration and has no side effects.
// Deployment tooling must persist this value once per release for all replicas;
// process startup must never use it to manufacture its own expected value.
func DeploymentContractDigestFromEnvironment() (string, error) {
	contract, err := deploymentPlatformContract(Config{
		PlatformBackendCatalogSHA256: strings.TrimSpace(os.Getenv("CONTROL_PLATFORM_BACKEND_CATALOG_SHA256")),
		PlatformBackendTargetsSHA256: strings.TrimSpace(os.Getenv("CONTROL_PLATFORM_BACKEND_TARGETS_SHA256")),
	})
	if err != nil {
		return "", err
	}
	return contract.Digest, nil
}

func validateExpectedDeploymentContractDigest(digest string) error {
	if digest == "" {
		return fmt.Errorf("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST is required")
	}
	if !deploymentContractDigestPattern.MatchString(digest) {
		return fmt.Errorf("CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST must be sha256: followed by 64 lowercase hexadecimal characters")
	}
	return nil
}

func checkedDeploymentPlatformContract(config Config) (deploymentdomain.PlatformExecutionContract, error) {
	if err := validateExpectedDeploymentContractDigest(config.DeploymentExpectedContractDigest); err != nil {
		return deploymentdomain.PlatformExecutionContract{}, err
	}
	contract, err := deploymentPlatformContract(config)
	if err != nil {
		return deploymentdomain.PlatformExecutionContract{}, err
	}
	if contract.Digest != config.DeploymentExpectedContractDigest {
		return deploymentdomain.PlatformExecutionContract{}, fmt.Errorf(
			"deployment platform contract digest mismatch: expected %s, actual %s",
			config.DeploymentExpectedContractDigest, contract.Digest,
		)
	}
	return contract, nil
}
