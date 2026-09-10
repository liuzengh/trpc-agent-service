package modelruntime

import (
	"context"
	"errors"
	"fmt"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

var (
	// ErrSecretResolution is returned when a resolver cannot provide a secret.
	ErrSecretResolution = errors.New("model secret resolution failed")
	// ErrModelFactory is returned when a Model Factory cannot build a model.
	ErrModelFactory = errors.New("model factory failed")
)

// ResolveAndBuild resolves the optional secret and passes it directly to the
// ModelFactory. Resolver and Factory errors are intentionally sanitized so
// provider credentials cannot escape through an error chain.
func ResolveAndBuild(ctx context.Context, input modelprofile.ModelFactoryInput, resolver modelprofile.SecretResolver, factory modelprofile.ModelFactory) (trpcmodel.Model, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", modelprofile.ErrInvalid)
	}
	if factory == nil {
		return nil, fmt.Errorf("%w: model factory is required", modelprofile.ErrInvalid)
	}
	if err := input.Validate(); err != nil {
		return nil, err
	}
	secret := modelprofile.SecretValue{}
	if input.SecretRef != "" {
		if resolver == nil {
			return nil, fmt.Errorf("%w: secret resolver is required", modelprofile.ErrInvalid)
		}
		scope := modelprofile.SecretScope{TenantID: input.TenantID, SecretRef: input.SecretRef}
		if err := scope.Validate(); err != nil {
			return nil, err
		}
		resolved, err := resolver.Resolve(ctx, scope)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrSecretResolution
		}
		secret = resolved
	}
	model, err := factory.New(ctx, input.Clone(), secret)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrModelFactory
	}
	if model == nil {
		return nil, fmt.Errorf("%w: returned nil model", ErrModelFactory)
	}
	return model, nil
}
