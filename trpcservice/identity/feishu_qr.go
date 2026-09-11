package identity

import (
	"context"
	"errors"
	"net/url"
	"strings"
)

// QR login constants for the Feishu embedded QR code SDK.
//
// The SDK renders the QR code inside the console page. The QR payload is the
// "goto" authorization URL; after the user confirms in the Feishu app the SDK
// posts a tmp_code back to the page, the page navigates to goto+tmp_code and
// Feishu finally redirects the browser to redirect_uri with code+state.
const (
	// DefaultFeishuQRAuthorizeURL is the documented legacy authorization
	// endpoint required by the QR SDK ("暂不支持新版登录流程").
	DefaultFeishuQRAuthorizeURL = "https://passport.feishu.cn/suite/passport/oauth/authorize"
	// DefaultFeishuQRSDKURL is the official QR SDK script.
	DefaultFeishuQRSDKURL = "https://lf-package-cn.feishucdn.com/obj/feishu-static/lark/passport/qrcode/LarkSSOSDKWebQRCode-1.0.3.js"
	// FeishuQRTTLSeconds is the lifetime advertised to the browser. The
	// authorization code issued by Feishu is valid for 5 minutes and can only be
	// used once, so the UI must offer a refresh before that window closes.
	FeishuQRTTLSeconds = 300
)

// QRLoginProvider is implemented by identity providers that can render an
// embedded QR code instead of redirecting the whole page to the provider.
type QRLoginProvider interface {
	IdentityProvider
	// BeginQR returns the "goto" authorization URL embedded in the QR code.
	BeginQR(request AuthRequest) (string, error)
	// ExchangeQR exchanges a code produced by the legacy QR SDK flow. Keeping
	// this separate prevents QR compatibility from changing normal OAuth login.
	ExchangeQR(ctx context.Context, exchange AuthExchange) (Identity, error)
}

// NormalizeFeishuURL validates an https endpoint and allows plain http only for
// loopback addresses (local development and self-hosted SDK copies).
func NormalizeFeishuURL(raw, fallback string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback, nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", errors.New("Feishu URL is not a valid URL")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname())) {
		return "", errors.New("Feishu URL must use https")
	}
	if parsed.Host == "" {
		return "", errors.New("Feishu URL is missing a host")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return "", errors.New("Feishu URL must not contain user information or a fragment")
	}
	return value, nil
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(host, "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// FeishuQRGoto builds the authorization URL embedded in the QR code.
// Parameter order follows the official sample; url.Values.Encode sorts keys
// alphabetically which matches client_id, redirect_uri, response_type, state.
func FeishuQRGoto(authorizeURL, appID, redirectURI, state string) (string, error) {
	authorizeURL = strings.TrimSpace(authorizeURL)
	appID = strings.TrimSpace(appID)
	redirectURI = strings.TrimSpace(redirectURI)
	state = strings.TrimSpace(state)
	if authorizeURL == "" || appID == "" || redirectURI == "" {
		return "", errors.New("QR login requires authorize URL, app id and redirect URI")
	}
	if state == "" {
		return "", errors.New("QR login requires a state")
	}
	parsed, err := url.Parse(authorizeURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("QR login authorize URL is invalid")
	}
	query := parsed.Query()
	query.Set("client_id", appID)
	query.Set("redirect_uri", redirectURI)
	query.Set("response_type", "code")
	query.Set("state", state)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
