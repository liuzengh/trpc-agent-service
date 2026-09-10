// Package wechat defines explicit boundaries for WeChat Public Account and
// Customer Service products. They intentionally do not implement or alias the
// WeCom self-built application provider.
package wechat

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalid reports malformed provider configuration or message input.
var ErrInvalid = errors.New("invalid wechat provider")

// Product identifies the WeChat product boundary.
type Product string

const (
	// ProductPublicAccount identifies an official/public account.
	ProductPublicAccount Product = "public_account"
	// ProductCustomerService identifies the customer-service API.
	ProductCustomerService Product = "customer_service"
)

// Message is the minimal provider-neutral outbound message.
type Message struct {
	ToUser string
	Text   string
}

// PublicConfig configures a public account provider.
type PublicConfig struct {
	AppID     string
	AppSecret string
}

// CustomerServiceConfig configures a customer-service provider.
type CustomerServiceConfig struct {
	AppID     string
	AppSecret string
}

// PublicSender is the public-account transport owned by the caller.
type PublicSender interface {
	SendPublic(context.Context, Message) (string, error)
}

// CustomerServiceSender is the customer-service transport owned by the caller.
type CustomerServiceSender interface {
	SendCustomerService(context.Context, Message) (string, error)
}

// PublicProvider is the Public Account API boundary. It cannot satisfy the
// CustomerServiceSender interface by construction.
type PublicProvider struct {
	config PublicConfig
	sender PublicSender
}

// NewPublicProvider constructs a public-account provider boundary.
func NewPublicProvider(config PublicConfig, sender PublicSender) (*PublicProvider, error) {
	if err := validateConfig(config.AppID, config.AppSecret); err != nil || sender == nil {
		return nil, fmt.Errorf("%w: public account configuration is invalid", ErrInvalid)
	}
	return &PublicProvider{config: config, sender: sender}, nil
}

// Product returns the public-account product marker.
func (p *PublicProvider) Product() Product { return ProductPublicAccount }

// Send sends one public-account message.
func (p *PublicProvider) Send(ctx context.Context, message Message) (string, error) {
	if p == nil || p.sender == nil || ctx == nil || !validMessage(message) {
		return "", ErrInvalid
	}
	return p.sender.SendPublic(ctx, message)
}

// CustomerServiceProvider is the WeChat Customer Service API boundary.
type CustomerServiceProvider struct {
	config CustomerServiceConfig
	sender CustomerServiceSender
}

// NewCustomerServiceProvider constructs a customer-service provider boundary.
func NewCustomerServiceProvider(config CustomerServiceConfig, sender CustomerServiceSender) (*CustomerServiceProvider, error) {
	if err := validateConfig(config.AppID, config.AppSecret); err != nil || sender == nil {
		return nil, fmt.Errorf("%w: customer service configuration is invalid", ErrInvalid)
	}
	return &CustomerServiceProvider{config: config, sender: sender}, nil
}

// Product returns the customer-service product marker.
func (p *CustomerServiceProvider) Product() Product { return ProductCustomerService }

// Send sends one customer-service message.
func (p *CustomerServiceProvider) Send(ctx context.Context, message Message) (string, error) {
	if p == nil || p.sender == nil || ctx == nil || !validMessage(message) {
		return "", ErrInvalid
	}
	return p.sender.SendCustomerService(ctx, message)
}

func validateConfig(appID, secret string) error {
	if strings.TrimSpace(appID) == "" || strings.TrimSpace(secret) == "" || strings.ContainsAny(appID, "\r\n") || strings.ContainsAny(secret, "\r\n") {
		return ErrInvalid
	}
	return nil
}

func validMessage(message Message) bool {
	return strings.TrimSpace(message.ToUser) != "" && strings.TrimSpace(message.Text) != ""
}
