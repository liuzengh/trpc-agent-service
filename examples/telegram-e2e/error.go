package main

import "errors"

var (
	errConfiguration         = errors.New("invalid Telegram E2E configuration")
	errPreflight             = errors.New("telegram E2E preflight failed")
	errPreflightClient       = errors.New("telegram E2E bot client preflight failed")
	errPreflightGetMeNetwork = errors.New("telegram E2E getMe network failure")
	errPreflightGetMeTimeout = errors.New("telegram E2E getMe timeout")
	errPreflightGetMeAPI     = errors.New("telegram E2E getMe Telegram API rejected the request")
	errPreflightGetMeReply   = errors.New("telegram E2E getMe response was invalid")
	errPreflightWebhook      = errors.New("telegram E2E webhook preflight failed")
	errWebhookConfigured     = errors.New("telegram webhook is configured; remove it or enable TELEGRAM_DELETE_WEBHOOK")
	errAdapterConfiguration  = errors.New("telegram E2E adapter configuration failed")
	errAdapterInitialization = errors.New("telegram E2E adapter initialization failed")
	errAdapterIdentity       = errors.New("telegram E2E adapter identity check failed")
	errAdapterRun            = errors.New("telegram E2E adapter stopped unexpectedly")
	errAdapterClose          = errors.New("telegram E2E adapter close failed")
	errRunTimeout            = errors.New("telegram E2E timed out waiting for the test message")
	errSender                = errors.New("telegram E2E sender failed")
	errSenderStopped         = errors.New("telegram E2E sender stopped unexpectedly")
)
