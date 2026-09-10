package wechat

import (
	"context"
	"errors"
	"testing"
)

type fakePublic struct{}

func (fakePublic) SendPublic(context.Context, Message) (string, error) { return "public-1", nil }

type fakeCustomer struct{}

func (fakeCustomer) SendCustomerService(context.Context, Message) (string, error) {
	return "customer-1", nil
}

var errSender = errors.New("sender failed")

type errorPublic struct{}

func (errorPublic) SendPublic(context.Context, Message) (string, error) { return "", errSender }

type errorCustomer struct{}

func (errorCustomer) SendCustomerService(context.Context, Message) (string, error) {
	return "", errSender
}

func TestProvidersKeepProductBoundaries(t *testing.T) {
	public, err := NewPublicProvider(PublicConfig{AppID: "public", AppSecret: "secret"}, fakePublic{})
	if err != nil {
		t.Fatal(err)
	}
	customer, err := NewCustomerServiceProvider(CustomerServiceConfig{AppID: "customer", AppSecret: "secret"}, fakeCustomer{})
	if err != nil {
		t.Fatal(err)
	}
	if public.Product() != ProductPublicAccount || customer.Product() != ProductCustomerService {
		t.Fatal("provider product boundary was not explicit")
	}
	if id, err := public.Send(context.Background(), Message{ToUser: "u", Text: "hello"}); err != nil || id != "public-1" {
		t.Fatalf("public send = %q, %v", id, err)
	}
	if id, err := customer.Send(context.Background(), Message{ToUser: "u", Text: "hello"}); err != nil || id != "customer-1" {
		t.Fatalf("customer send = %q, %v", id, err)
	}
}

func TestWeChatProviderConstructorsRejectInvalidInputs(t *testing.T) {
	publicCases := []struct {
		name   string
		config PublicConfig
		sender PublicSender
	}{
		{name: "missing app id", config: PublicConfig{AppSecret: "secret"}, sender: fakePublic{}},
		{name: "missing app secret", config: PublicConfig{AppID: "app"}, sender: fakePublic{}},
		{name: "newline app id", config: PublicConfig{AppID: "app\n", AppSecret: "secret"}, sender: fakePublic{}},
		{name: "nil sender", config: PublicConfig{AppID: "app", AppSecret: "secret"}},
	}
	for _, test := range publicCases {
		t.Run("public/"+test.name, func(t *testing.T) {
			if _, err := NewPublicProvider(test.config, test.sender); !errors.Is(err, ErrInvalid) {
				t.Fatalf("constructor error = %v, want ErrInvalid", err)
			}
		})
	}
	customerCases := []struct {
		name   string
		config CustomerServiceConfig
		sender CustomerServiceSender
	}{
		{name: "missing app id", config: CustomerServiceConfig{AppSecret: "secret"}, sender: fakeCustomer{}},
		{name: "missing app secret", config: CustomerServiceConfig{AppID: "app"}, sender: fakeCustomer{}},
		{name: "newline secret", config: CustomerServiceConfig{AppID: "app", AppSecret: "secret\r"}, sender: fakeCustomer{}},
		{name: "nil sender", config: CustomerServiceConfig{AppID: "app", AppSecret: "secret"}},
	}
	for _, test := range customerCases {
		t.Run("customer/"+test.name, func(t *testing.T) {
			if _, err := NewCustomerServiceProvider(test.config, test.sender); !errors.Is(err, ErrInvalid) {
				t.Fatalf("constructor error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestWeChatProviderSendValidationAndErrorPaths(t *testing.T) {
	public, err := NewPublicProvider(PublicConfig{AppID: "app", AppSecret: "secret"}, errorPublic{})
	if err != nil {
		t.Fatal(err)
	}
	customer, err := NewCustomerServiceProvider(CustomerServiceConfig{AppID: "app", AppSecret: "secret"}, errorCustomer{})
	if err != nil {
		t.Fatal(err)
	}
	valid := Message{ToUser: "user", Text: "hello"}
	if _, err := public.Send(context.Background(), valid); !errors.Is(err, errSender) {
		t.Fatalf("public sender error = %v, want sender error", err)
	}
	if _, err := customer.Send(context.Background(), valid); !errors.Is(err, errSender) {
		t.Fatalf("customer sender error = %v, want sender error", err)
	}
	publicInvalid := []struct {
		name string
		p    *PublicProvider
		ctx  context.Context
		msg  Message
	}{
		{name: "nil provider", msg: valid},
		{name: "nil sender", p: &PublicProvider{}, ctx: context.Background(), msg: valid},
		{name: "nil context", p: public, msg: valid},
		{name: "blank user", p: public, ctx: context.Background(), msg: Message{Text: "hello"}},
		{name: "blank text", p: public, ctx: context.Background(), msg: Message{ToUser: "user"}},
	}
	for _, test := range publicInvalid {
		t.Run("public/"+test.name, func(t *testing.T) {
			if _, err := test.p.Send(test.ctx, test.msg); !errors.Is(err, ErrInvalid) {
				t.Fatalf("send error = %v, want ErrInvalid", err)
			}
		})
	}
	customerInvalid := []struct {
		name string
		p    *CustomerServiceProvider
		ctx  context.Context
		msg  Message
	}{
		{name: "nil provider", msg: valid},
		{name: "nil sender", p: &CustomerServiceProvider{}, ctx: context.Background(), msg: valid},
		{name: "nil context", p: customer, msg: valid},
		{name: "blank user", p: customer, ctx: context.Background(), msg: Message{Text: "hello"}},
		{name: "blank text", p: customer, ctx: context.Background(), msg: Message{ToUser: "user"}},
	}
	for _, test := range customerInvalid {
		t.Run("customer/"+test.name, func(t *testing.T) {
			if _, err := test.p.Send(test.ctx, test.msg); !errors.Is(err, ErrInvalid) {
				t.Fatalf("send error = %v, want ErrInvalid", err)
			}
		})
	}
	var nilPublic *PublicProvider
	var nilCustomer *CustomerServiceProvider
	if nilPublic.Product() != ProductPublicAccount || nilCustomer.Product() != ProductCustomerService {
		t.Fatal("nil provider Product methods changed their stable markers")
	}
}
