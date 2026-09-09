package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telegramingress"
)

func main() {
	config, err := configFromEnv(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	poller, err := telegramingress.New(config, func(event telegramingress.Event) {
		log.Printf("telegram_ingress operation=%s outcome=%s status=%d update_id=%d", event.Operation, event.Outcome, event.Status, event.UpdateID)
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := poller.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
	log.Print("telegram_ingress stopped")
}

func configFromEnv(get func(string) string) (telegramingress.Config, error) {
	config := telegramingress.Config{
		BotToken:       get("TRPC_SECRET_TELEGRAM_BOT_TOKEN"),
		WebhookSecret:  get("TRPC_SECRET_TELEGRAM_WEBHOOK"),
		GatewayBaseURL: get("TELEGRAM_GATEWAY_BASE_URL"),
		RouteKey:       get("TELEGRAM_WEBHOOK_ROUTE_KEY"),
		StatePath:      get("TELEGRAM_STATE_PATH"),
		PollTimeout:    30 * time.Second,
	}
	if value := get("TELEGRAM_EXPECTED_BOT_ID"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return config, telegramingress.ErrConfiguration
		}
		config.ExpectedBotID = parsed
	}
	if value := get("TELEGRAM_POLL_TIMEOUT"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return config, telegramingress.ErrConfiguration
		}
		config.PollTimeout = parsed
	}
	return config, nil
}
