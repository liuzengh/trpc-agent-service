// Package modelruntime contains runtime-only model resolution and provider
// materialization. The model package remains the owner of the persisted
// profile and the provider-neutral execution contracts.
package modelruntime

import modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"

// These aliases keep the runtime adapter on the same provider-neutral
// contracts without moving domain state into this package.
type (
	// ModelFactory constructs an upstream model from a provider-neutral input.
	ModelFactory = modelprofile.ModelFactory
	// ModelFactoryInput is the secret-free model construction input.
	ModelFactoryInput = modelprofile.ModelFactoryInput
	// SecretManager reads one tenant-scoped secret for a runtime adapter.
	SecretManager = modelprofile.SecretManager
	// SecretResolver resolves one tenant-scoped secret.
	SecretResolver = modelprofile.SecretResolver
	// SecretScope identifies one tenant-scoped secret reference.
	SecretScope = modelprofile.SecretScope
	// SecretValue is the temporary, redacted secret value passed to a factory.
	SecretValue = modelprofile.SecretValue
)

// ErrInvalid is the model-domain validation sentinel.
var ErrInvalid = modelprofile.ErrInvalid

// SchemaVersionV1 is the first supported Model Profile configuration schema.
const SchemaVersionV1 = modelprofile.SchemaVersionV1

// NewSecretValue delegates construction to the model-domain secret value
// contract while keeping runtime registry callers on this package boundary.
func NewSecretValue(value string) (SecretValue, error) { return modelprofile.NewSecretValue(value) }
