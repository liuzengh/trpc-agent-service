package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/gowebpki/jcs"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type mutation func(context.Context, Transaction) (CommandResult, error)
type preparation func(context.Context) (mutation, error)

func (s *Service) authorize(ctx context.Context, actor Actor, owner bool) error {
	if !domain.ValidID(actor.TenantID) || !domain.ValidID(actor.UserID) {
		return ErrPermissionDenied
	}
	allowed, err := s.deps.TenantAccess.IsActiveMember(ctx, actor.TenantID, actor.UserID)
	if err != nil {
		return ErrDependencyUnavailable
	}
	if !allowed {
		return ErrPermissionDenied
	}
	if owner {
		allowed, err = s.deps.TenantAccess.IsActiveOwner(ctx, actor.TenantID, actor.UserID)
		if err != nil {
			return ErrDependencyUnavailable
		}
		if !allowed {
			return ErrPermissionDenied
		}
	}
	return nil
}
func validKey(v string) bool {
	if len(v) < 1 || len(v) > 128 {
		return false
	}
	for _, r := range v {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func (s *Service) execute(ctx context.Context, actor Actor, operation, scope, key string, input any, prepare preparation) (CommandResult, error) {
	if err := s.authorize(ctx, actor, true); err != nil {
		return CommandResult{}, err
	}
	if !validKey(key) {
		return CommandResult{}, invalid("/Idempotency-Key")
	}
	rawCommand, err := json.Marshal(struct {
		Operation string `json:"operation"`
		TenantID  string `json:"tenant_id"`
		ActorID   string `json:"actor_id"`
		ScopeID   string `json:"scope_id"`
		Input     any    `json:"input"`
	}{operation, actor.TenantID, actor.UserID, scope, input})
	if err != nil {
		return CommandResult{}, err
	}
	defer clear(rawCommand)
	canonical, err := jcs.Transform(rawCommand)
	if err != nil {
		return CommandResult{}, invalid("")
	}
	defer clear(canonical)
	sum := sha256.Sum256([]byte(key))
	receiptKey := ReceiptKey{actor.TenantID, operation, scope, hex.EncodeToString(sum[:])}
	for attempt := 0; attempt < 3; attempt++ {
		_, found, err := s.deps.Commands.FindReceipt(ctx, receiptKey)
		if err != nil {
			return CommandResult{}, err
		}
		var apply mutation
		if !found {
			apply, err = prepare(ctx)
			if err != nil {
				return CommandResult{}, err
			}
		}
		var output CommandResult
		err = s.deps.Commands.WithWrite(ctx, WriteScope{ScopeID: s.deps.ScopeID, Actor: actor}, func(tx Transaction) error {
			receipt, found, err := tx.FindReceipt(ctx, receiptKey)
			if err != nil {
				return err
			}
			if found {
				match, err := s.deps.Cipher.VerifyRequest(ctx, receipt.MACKeyID, receipt.RequestMAC, canonical)
				if err != nil {
					return ErrDependencyUnavailable
				}
				if !match {
					return ErrIdempotencyConflict
				}
				if receipt.CreatedBy != actor.UserID || receipt.Key != receiptKey {
					return &domain.Error{Code: domain.SourceIntegrity}
				}
				if err = json.Unmarshal(receipt.Result, &output); err != nil {
					return &domain.Error{Code: domain.SourceIntegrity}
				}
				return nil
			}
			if apply == nil {
				return errPreparationChanged
			}
			output, err = apply(ctx, tx)
			if err != nil {
				return err
			}
			raw, _, err := domain.CanonicalJSON(output)
			if err != nil {
				return err
			}
			keyID, mac, err := s.deps.Cipher.SignRequest(ctx, canonical)
			if err != nil {
				return ErrDependencyUnavailable
			}
			return tx.SaveReceipt(ctx, Receipt{Key: receiptKey, MACKeyID: keyID, RequestMAC: mac, Result: raw, CreatedBy: actor.UserID, CreatedAt: s.deps.Now()})
		})
		if errors.Is(err, errPreparationChanged) {
			continue
		}
		return output, err
	}
	return CommandResult{}, ErrDependencyUnavailable
}
func (s *Service) project(ctx context.Context, tx Transaction, a Aggregate) (CommandResult, error) {
	eventID, err := s.deps.NewID("evt")
	if err != nil {
		return CommandResult{}, ErrDependencyUnavailable
	}
	next, changed, err := domain.AdvanceRoute(a.Route, a.Account, a.Binding, eventID)
	if err != nil {
		return CommandResult{}, err
	}
	if changed {
		a.Route = next
		a.Account.MinRouteGeneration = next.Generation
	}
	if err = tx.Save(ctx, a); err != nil {
		return CommandResult{}, err
	}
	output := result(a)
	if changed {
		output.EventID = next.Projection.EventID
		output.Distribution = "PENDING"
	}
	return output, nil
}
func (s *Service) readTarget(ctx context.Context, actor Actor, selector domain.TargetSelector) (domain.PublishedTarget, error) {
	if !domain.ValidID(selector.DeploymentID) || !domain.ValidVersion(selector.RevisionNumber) {
		return domain.PublishedTarget{}, invalid("/target")
	}
	target, err := s.deps.Targets.ReadExact(ctx, actor.TenantID, actor.UserID, selector)
	if err != nil {
		return target, err
	}
	if err = target.Validate(actor.TenantID); err != nil {
		return target, err
	}
	if target.Selector() != selector {
		return target, &domain.Error{Code: domain.TargetIntegrity}
	}
	return target, nil
}

// CreateAccountInput never accepts a tenant, endpoint, credential reference or
// enabled flag. Physical identity normalization precedes MAC canonicalization.
type AccountConfigInput struct {
	ReceiveMode     string  `json:"receive_mode"`
	EndpointProfile *string `json:"endpoint_profile,omitempty"`
}

type CreateAccountInput struct {
	Config            *AccountConfigInput              `json:"config,omitempty"`
	LegacyWebhook     bool                             `json:"-"`
	Provider          domain.Provider                  `json:"provider"`
	ProviderAccountID string                           `json:"provider_account_id"`
	Name              string                           `json:"name"`
	Description       string                           `json:"description"`
	Credentials       map[string]domain.CredentialEdit `json:"credentials"`
}

func (s *Service) CreateAccount(ctx context.Context, actor Actor, key string, input CreateAccountInput) (CommandResult, error) {
	if err := s.authorize(ctx, actor, true); err != nil {
		return CommandResult{}, err
	}
	physical, err := domain.NormalizeProviderAccountID(input.Provider, input.ProviderAccountID)
	if err != nil {
		return CommandResult{}, err
	}
	input.ProviderAccountID = physical
	input.Name = strings.TrimSpace(input.Name)
	mode := domain.LongPolling
	if input.LegacyWebhook {
		if input.Provider != domain.Telegram || input.Config != nil {
			return CommandResult{}, invalid("/X-Channel-Create-Contract")
		}
		mode = domain.Webhook
	} else if input.Provider == domain.Telegram {
		if input.Config != nil && input.Config.ReceiveMode != "" {
			mode = input.Config.ReceiveMode
		}
		if !domain.ValidReceiveMode(mode) {
			return CommandResult{}, invalid("/config/receive_mode")
		}
		if input.Config == nil {
			input.Config = &AccountConfigInput{}
		}
		input.Config.ReceiveMode = mode
		if input.Config.EndpointProfile != nil && *input.Config.EndpointProfile != "official" && *input.Config.EndpointProfile != "test" {
			return CommandResult{}, invalid("/config/endpoint_profile")
		}
	} else if input.Config != nil {
		return CommandResult{}, invalid("/config")
	}
	required := domain.RequiredPurposes(input.Provider, mode)
	allowed := domain.AllowedPurposes(input.Provider)
	if len(input.Credentials) < len(required) || len(input.Credentials) > len(allowed) {
		return CommandResult{}, invalid("/credentials")
	}
	for _, purpose := range required {
		if _, ok := input.Credentials[purpose]; !ok {
			return CommandResult{}, invalid("/credentials")
		}
	}
	for purpose, edit := range input.Credentials {
		if !slices.Contains(allowed, purpose) || edit.Action != "replace" {
			return CommandResult{}, invalid("/credentials")
		}
		if err = edit.Validate(input.Provider, purpose); err != nil {
			return CommandResult{}, err
		}
	}
	return s.execute(ctx, actor, "CreateChannelAccount", s.deps.ScopeID, key, input, func(ctx context.Context) (mutation, error) {
		id, err := s.deps.NewID("cha")
		if err != nil {
			return nil, ErrDependencyUnavailable
		}
		a, err := domain.NewAccount(actor.TenantID, id, s.deps.ScopeID, actor.UserID, input.Provider, physical, input.Name, input.Description, s.deps.Now(), mode)
		if err != nil {
			return nil, err
		}
		if input.Config != nil && input.Config.EndpointProfile != nil && *input.Config.EndpointProfile == "test" {
			a.Config.EndpointProfile = "test"
		}
		records := make([]domain.CredentialRecord, 0, len(allowed))
		for _, purpose := range allowed {
			id, err := s.deps.NewID("ccr")
			if err != nil {
				return nil, ErrDependencyUnavailable
			}
			records = append(records, domain.CredentialRecord{TenantID: a.TenantID, AccountID: a.ID, Provider: a.Provider, Meta: domain.CredentialMeta{Purpose: purpose, ID: id, Version: 1, Configured: input.Credentials[purpose].Action == "replace"}})
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			for i := range records {
				if !records[i].Meta.Configured {
					continue
				}
				aad, err := records[i].AAD()
				if err != nil {
					return CommandResult{}, err
				}
				value := []byte(*input.Credentials[records[i].Meta.Purpose].Value)
				keyID, ciphertext, err := s.deps.Cipher.Encrypt(ctx, aad, value)
				clear(value)
				if err != nil {
					return CommandResult{}, ErrDependencyUnavailable
				}
				records[i].KeyID = keyID
				records[i].Ciphertext = ciphertext
			}
			aggregate := Aggregate{Account: a, Credentials: records, Route: domain.RouteState{TenantID: a.TenantID, AccountID: a.ID}}
			if err = tx.Save(ctx, aggregate); err != nil {
				return CommandResult{}, err
			}
			return result(aggregate), nil
		}, nil
	})
}

type UpdateAccountInput struct {
	Config                  *AccountConfigInput `json:"config,omitempty"`
	ExpectedAccountRevision int64               `json:"expected_account_revision"`
	Name                    *string             `json:"name,omitempty"`
	Description             *string             `json:"description,omitempty"`
}

func (s *Service) UpdateAccount(ctx context.Context, actor Actor, id, key string, input UpdateAccountInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/account_id")
	}
	if input.Name != nil {
		v := strings.TrimSpace(*input.Name)
		input.Name = &v
	}
	return s.execute(ctx, actor, "UpdateChannelAccount", id, key, input, func(context.Context) (mutation, error) {
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadAccount(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			var mode *string
			if input.Config != nil {
				mode = &input.Config.ReceiveMode
			}
			var endpoint []string
			if input.Config != nil && input.Config.EndpointProfile != nil {
				endpoint = []string{*input.Config.EndpointProfile}
			}
			if mode != nil && *mode == "" {
				mode = nil
			}
			updated, changed, err := a.Account.ChangeConfiguration(input.ExpectedAccountRevision, input.Name, input.Description, mode, s.deps.Now(), endpoint...)
			if err != nil {
				return CommandResult{}, err
			}
			if changed {
				a.Account = updated
				if err = tx.Save(ctx, a); err != nil {
					return CommandResult{}, err
				}
			}
			return result(a), nil
		}, nil
	})
}

type UpdateCredentialInput struct {
	ExpectedAccountRevision   int64 `json:"expected_account_revision"`
	ExpectedCredentialVersion int64 `json:"expected_credential_version"`
	domain.CredentialEdit
}

func (s *Service) UpdateCredential(ctx context.Context, actor Actor, id, purpose, key string, input UpdateCredentialInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/account_id")
	}
	request := struct {
		Purpose string `json:"purpose"`
		UpdateCredentialInput
	}{purpose, input}
	return s.execute(ctx, actor, "UpdateAccountCredential", id, key, request, func(context.Context) (mutation, error) {
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadAccount(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			index := -1
			for i, c := range a.Credentials {
				if c.Meta.Purpose == purpose {
					index = i
					break
				}
			}
			if index < 0 {
				return CommandResult{}, invalid("/purpose")
			}
			next, credential, changed, err := domain.PlanCredentialUpdate(a.Account, a.Credentials[index], input.ExpectedAccountRevision, input.ExpectedCredentialVersion, input.CredentialEdit, s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			if changed {
				if credential.Meta.Configured {
					aad, err := credential.AAD()
					if err != nil {
						return CommandResult{}, err
					}
					value := []byte(*input.Value)
					keyID, ciphertext, err := s.deps.Cipher.Encrypt(ctx, aad, value)
					clear(value)
					if err != nil {
						return CommandResult{}, ErrDependencyUnavailable
					}
					credential.KeyID = keyID
					credential.Ciphertext = ciphertext
				}
				a.Account = next
				a.Credentials[index] = credential
				if err = tx.Save(ctx, a); err != nil {
					return CommandResult{}, err
				}
			}
			return result(a), nil
		}, nil
	})
}

type AccountEnabledInput struct {
	ExpectedAccountRevision int64 `json:"expected_account_revision"`
	Enabled                 bool  `json:"enabled"`
}

func (s *Service) SetAccountEnabled(ctx context.Context, actor Actor, id, key string, input AccountEnabledInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/account_id")
	}
	return s.execute(ctx, actor, "SetChannelAccountEnabled", id, key, input, func(ctx context.Context) (mutation, error) {
		var prepared *domain.PublishedTarget
		var preparedCanary *domain.PublishedTarget
		if input.Enabled {
			a, err := s.deps.Queries.GetAccount(ctx, actor.TenantID, id)
			if err != nil {
				return nil, err
			}
			if a.Binding != nil && a.Binding.Enabled {
				target, err := s.readTarget(ctx, actor, a.Binding.Target.Selector())
				if err != nil {
					return nil, err
				}
				if target != a.Binding.Target {
					return nil, &domain.Error{Code: domain.TargetIntegrity}
				}
				prepared = &target
				if a.Binding.Traffic != nil {
					canary, err := s.readTarget(ctx, actor, a.Binding.Traffic.Target.Selector())
					if err != nil {
						return nil, err
					}
					if canary != a.Binding.Traffic.Target {
						return nil, &domain.Error{Code: domain.TargetIntegrity}
					}
					preparedCanary = &canary
				}
			}
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadAccount(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			if input.Enabled && a.Binding != nil && a.Binding.Enabled {
				if prepared == nil || *prepared != a.Binding.Target {
					return CommandResult{}, errPreparationChanged
				}
				if (a.Binding.Traffic == nil) != (preparedCanary == nil) {
					return CommandResult{}, errPreparationChanged
				}
				if a.Binding.Traffic != nil && *preparedCanary != a.Binding.Traffic.Target {
					return CommandResult{}, errPreparationChanged
				}
			}
			next, changed, err := a.Account.SetEnabled(input.ExpectedAccountRevision, input.Enabled, a.CredentialMetadata(), s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			if !changed {
				return result(a), nil
			}
			a.Account = next
			return s.project(ctx, tx, a)
		}, nil
	})
}

type CreateBindingInput struct {
	AccountID string                `json:"account_id"`
	Target    domain.TargetSelector `json:"target"`
}

func (s *Service) CreateBinding(ctx context.Context, actor Actor, key string, input CreateBindingInput) (CommandResult, error) {
	if !domain.ValidID(input.AccountID) {
		return CommandResult{}, invalid("/account_id")
	}
	return s.execute(ctx, actor, "CreateChannelBinding", input.AccountID, key, input, func(ctx context.Context) (mutation, error) {
		target, err := s.readTarget(ctx, actor, input.Target)
		if err != nil {
			return nil, err
		}
		id, err := s.deps.NewID("chb")
		if err != nil {
			return nil, ErrDependencyUnavailable
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadAccount(ctx, input.AccountID)
			if err != nil {
				return CommandResult{}, err
			}
			if a.Binding != nil {
				return CommandResult{}, ErrAccountAlreadyBound
			}
			binding, err := domain.NewBinding(id, actor.UserID, a.Account, target, s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			a.Binding = &binding
			return s.project(ctx, tx, a)
		}, nil
	})
}

type BindingTargetInput struct {
	ExpectedBindingRevision int64                 `json:"expected_binding_revision"`
	Target                  domain.TargetSelector `json:"target"`
}

type BindingTrafficInput struct {
	ExpectedBindingRevision int64                 `json:"expected_binding_revision"`
	Target                  domain.TargetSelector `json:"target"`
	PercentageBasisPoints   int64                 `json:"percentage_basis_points"`
	CanarySubjects          []string              `json:"canary_subjects"`
}

func (s *Service) SetBindingTraffic(ctx context.Context, actor Actor, id, key string, input BindingTrafficInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/binding_id")
	}
	return s.execute(ctx, actor, "SetChannelBindingTraffic", id, key, input, func(ctx context.Context) (mutation, error) {
		target, err := s.readTarget(ctx, actor, input.Target)
		if err != nil {
			return nil, err
		}
		rolloutID, err := s.deps.NewID("rol")
		if err != nil {
			return nil, ErrDependencyUnavailable
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadBinding(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			binding, changed, err := a.Binding.SetTraffic(input.ExpectedBindingRevision, rolloutID, target, input.PercentageBasisPoints, input.CanarySubjects, s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			if !changed {
				return result(a), nil
			}
			a.Binding = &binding
			return s.project(ctx, tx, a)
		}, nil
	})
}

func (s *Service) SetBindingTarget(ctx context.Context, actor Actor, id, key string, input BindingTargetInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/binding_id")
	}
	return s.execute(ctx, actor, "SetChannelBindingTarget", id, key, input, func(ctx context.Context) (mutation, error) {
		target, err := s.readTarget(ctx, actor, input.Target)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadBinding(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			binding, changed, err := a.Binding.SetTarget(input.ExpectedBindingRevision, target, s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			if !changed {
				return result(a), nil
			}
			a.Binding = &binding
			return s.project(ctx, tx, a)
		}, nil
	})
}

type BindingEnabledInput struct {
	ExpectedBindingRevision int64 `json:"expected_binding_revision"`
	Enabled                 bool  `json:"enabled"`
}

func (s *Service) SetBindingEnabled(ctx context.Context, actor Actor, id, key string, input BindingEnabledInput) (CommandResult, error) {
	if !domain.ValidID(id) {
		return CommandResult{}, invalid("/binding_id")
	}
	return s.execute(ctx, actor, "SetChannelBindingEnabled", id, key, input, func(ctx context.Context) (mutation, error) {
		var prepared *domain.PublishedTarget
		var preparedCanary *domain.PublishedTarget
		if input.Enabled {
			a, err := s.deps.Queries.GetBinding(ctx, actor.TenantID, id)
			if err != nil {
				return nil, err
			}
			if a.Binding == nil {
				return nil, ErrBindingNotFound
			}
			target, err := s.readTarget(ctx, actor, a.Binding.Target.Selector())
			if err != nil {
				return nil, err
			}
			if target != a.Binding.Target {
				return nil, &domain.Error{Code: domain.TargetIntegrity}
			}
			prepared = &target
			if a.Binding.Traffic != nil {
				canary, err := s.readTarget(ctx, actor, a.Binding.Traffic.Target.Selector())
				if err != nil {
					return nil, err
				}
				if canary != a.Binding.Traffic.Target {
					return nil, &domain.Error{Code: domain.TargetIntegrity}
				}
				preparedCanary = &canary
			}
		}
		return func(ctx context.Context, tx Transaction) (CommandResult, error) {
			a, err := tx.LoadBinding(ctx, id)
			if err != nil {
				return CommandResult{}, err
			}
			if input.Enabled && (prepared == nil || *prepared != a.Binding.Target) {
				return CommandResult{}, errPreparationChanged
			}
			if input.Enabled && (a.Binding.Traffic == nil) != (preparedCanary == nil) {
				return CommandResult{}, errPreparationChanged
			}
			if input.Enabled && a.Binding.Traffic != nil && *preparedCanary != a.Binding.Traffic.Target {
				return CommandResult{}, errPreparationChanged
			}
			binding, changed, err := a.Binding.SetEnabled(input.ExpectedBindingRevision, input.Enabled, a.Account, a.CredentialMetadata(), s.deps.Now())
			if err != nil {
				return CommandResult{}, err
			}
			if !changed {
				return result(a), nil
			}
			a.Binding = &binding
			return s.project(ctx, tx, a)
		}, nil
	})
}

// AuthorizeWrite lets inbound adapters reject unauthorized bodies before parsing.
// Each command and its transaction independently recheck the same authorization.
func (s *Service) AuthorizeWrite(ctx context.Context, actor Actor) error {
	return s.authorize(ctx, actor, true)
}
