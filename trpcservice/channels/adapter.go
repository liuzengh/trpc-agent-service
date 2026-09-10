package channels

import (
	"context"
	"net/http"
)

// Adapter is the lifecycle boundary shared by every tenant-scoped IM channel
// implementation. Protocol authentication and transport ownership stay with
// the concrete adapter; all accepted messages use the gateway's
// protocol-neutral InboundMessage contract. Delivery providers are kept in the
// composition-only channels/provider package so the channel domain itself does
// not depend on the runtime outbox.
type Adapter interface {
	Channel() Channel
	Close() error
}

// PollingAdapter is an Adapter whose concrete protocol owns a blocking poll
// loop. The caller owns the supplied Context and must cancel it during process
// shutdown before calling Close.
type PollingAdapter interface {
	Adapter
	Run(context.Context) error
}

// WebhookAdapter is an Adapter whose concrete protocol owns an HTTP ingress
// boundary. BeginShutdown stops new admissions before Close joins accepted
// protocol work.
type WebhookAdapter interface {
	Adapter
	http.Handler
	BeginShutdown()
}
