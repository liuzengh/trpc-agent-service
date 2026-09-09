// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"errors"
	"fmt"
	"regexp"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

var (
	// ErrChannelCredentialInvalid is stable and never contains credential data.
	ErrChannelCredentialInvalid = errors.New("channel credential is invalid")
	// ErrChannelRequestBuild identifies a local provider request construction failure.
	ErrChannelRequestBuild = errors.New("channel provider request could not be built")
	// ErrChannelTransport identifies an outbound provider transport failure.
	ErrChannelTransport = errors.New("channel provider transport failed")
	// ErrInvalidOutboundMessage identifies a message that cannot be represented
	// by the provider. It is permanent: retrying the same malformed payload
	// would only create a durable delivery storm.
	ErrInvalidOutboundMessage = errors.New("outbound channel message is invalid")
)

var weWorkAccessTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]{1,512}$`)

func validateTelegramDeliveryBinding(binding *tenant.ChannelBinding) error {
	if binding == nil || !tenant.IsValidTelegramBotToken(binding.Token) {
		return fmt.Errorf("%w: Telegram", ErrChannelCredentialInvalid)
	}
	return nil
}

func validateWeWorkDeliveryBinding(binding *tenant.ChannelBinding) error {
	if binding == nil ||
		!tenant.IsValidWeWorkCorpID(binding.Config["corp_id"]) ||
		!tenant.IsValidWeWorkCorpSecret(binding.Secret) ||
		!tenant.IsValidWeWorkAgentID(binding.AppID) {
		return fmt.Errorf("%w: WeWork", ErrChannelCredentialInvalid)
	}
	return nil
}

func validWeWorkAccessToken(value string) bool {
	return weWorkAccessTokenPattern.MatchString(value)
}

func permanentCredentialError(provider string) error {
	return PermanentDeliveryError(fmt.Errorf("%w: %s", ErrChannelCredentialInvalid, provider))
}

func permanentRequestBuildError(provider string) error {
	return PermanentDeliveryError(fmt.Errorf("%w: %s", ErrChannelRequestBuild, provider))
}

func retryableTransportError(provider string) error {
	return RetryableDeliveryError(fmt.Errorf("%w: %s", ErrChannelTransport, provider))
}

func invalidOutboundMessageError(provider string) error {
	return PermanentDeliveryError(fmt.Errorf("%w: %s", ErrInvalidOutboundMessage, provider))
}
