package channels

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

const maxDeliveryResponseBytes = 1 << 20

// httpExchange is deliberately bounded and contains no request URL. Telegram
// embeds its credential in that URL, so neither transport errors nor malformed
// URL errors may escape through Result.Err.
type httpExchange struct {
	statusCode   int
	header       http.Header
	body         []byte
	responseHash string
	tooLarge     bool
	err          error
	// requestCreated distinguishes local encoding/construction failures (known
	// not sent) from a client.Do failure, whose delivery result is uncertain.
	requestCreated bool
}

func postJSON(ctx context.Context, client HTTPDoer, url string, headers map[string]string, payload any) httpExchange {
	body, err := json.Marshal(payload)
	if err != nil {
		return httpExchange{err: errors.New("encode delivery payload failed")}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		// The Telegram API embeds the bot token in the URL. Never propagate a
		// URL-bearing parser error into logs, traces or audit records.
		return httpExchange{err: errors.New("create delivery request failed")}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return httpExchange{requestCreated: true, err: errors.New("delivery network request failed")}
	}
	return readHTTPResponse(resp)
}

func readHTTPResponse(resp *http.Response) httpExchange {
	defer resp.Body.Close()
	bounded, readErr := io.ReadAll(io.LimitReader(resp.Body, maxDeliveryResponseBytes+1))
	tooLarge := len(bounded) > maxDeliveryResponseBytes
	if tooLarge {
		bounded = bounded[:maxDeliveryResponseBytes]
	}
	exchange := httpExchange{
		statusCode:     resp.StatusCode,
		header:         resp.Header.Clone(),
		body:           bounded,
		responseHash:   fmt.Sprintf("%x", sha256.Sum256(bounded)),
		tooLarge:       tooLarge,
		requestCreated: true,
	}
	if readErr != nil {
		exchange.err = errors.New("read delivery response failed")
	}
	return exchange
}

func chunks(text string, limit int) []string {
	if limit <= 0 {
		limit = 4000
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	result := make([]string, 0, (len(runes)+limit-1)/limit)
	for len(runes) > 0 {
		n := limit
		if len(runes) < n {
			n = len(runes)
		}
		result = append(result, string(runes[:n]))
		runes = runes[n:]
	}
	return result
}

func planRuneParts(msg domain.OutboundMessage, limit int) []delivery.Part {
	pieces := chunks(msg.Text, limit)
	result := make([]delivery.Part, 0, len(pieces))
	for index, text := range pieces {
		partMessage := cloneOutbound(msg)
		partMessage.Text = text
		result = append(result, delivery.Part{Message: partMessage, Index: index, Total: len(pieces)})
	}
	return result
}

func cloneOutbound(msg domain.OutboundMessage) domain.OutboundMessage {
	cloned := msg
	cloned.Attachments = append([]domain.Attachment(nil), msg.Attachments...)
	return cloned
}

func decodeJSONResponse(exchange httpExchange, target any) error {
	if exchange.err != nil {
		return exchange.err
	}
	if exchange.tooLarge {
		return errors.New("delivery response exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(exchange.body))
	if err := decoder.Decode(target); err != nil {
		return errors.New("decode delivery response failed")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("decode delivery response failed")
	}
	return nil
}

func responseMetadata(exchange httpExchange, requestHeaders ...string) delivery.Result {
	result := delivery.Result{
		HTTPStatus:   exchange.statusCode,
		ResponseHash: exchange.responseHash,
	}
	for _, name := range requestHeaders {
		if value := sanitizeIdentifier(exchange.header.Get(name), 160); value != "" {
			result.ProviderRequestID = value
			break
		}
	}
	return result
}

func classifyHTTP(exchange httpExchange, now time.Time, requestHeaders ...string) (delivery.Result, bool) {
	result := responseMetadata(exchange, requestHeaders...)
	if exchange.err != nil {
		if exchange.requestCreated {
			result.Outcome = delivery.Unknown
			result.ErrorType = "network_unknown"
		} else {
			result.Outcome = delivery.PermanentRejected
			result.ErrorType = "request_invalid"
		}
		result.Err = exchange.err
		return result, true
	}
	status := exchange.statusCode
	if status >= 200 && status < 300 {
		return result, false
	}
	result.ProviderCode = strconv.Itoa(status)
	result.RetryAfter = parseRetryAfter(exchange.header.Get("Retry-After"), now)
	switch {
	case status == http.StatusTooManyRequests:
		result.Outcome = delivery.RetryableNotSent
		result.ErrorType = "provider_transient"
	case status == http.StatusRequestTimeout || status >= 500:
		// A generic server/timeout response does not prove that the provider
		// failed before creating the message. A provider-specific, parseable
		// rejection may override this conservatively in the adapter.
		result.Outcome = delivery.Unknown
		result.ErrorType = "provider_ambiguous"
	case status >= 300:
		result.Outcome = delivery.PermanentRejected
		result.ErrorType = "provider_rejected"
	default:
		result.Outcome = delivery.Unknown
		result.ErrorType = "response_invalid"
	}
	return result, true
}

func malformedSuccess(exchange httpExchange, requestHeaders ...string) delivery.Result {
	result := responseMetadata(exchange, requestHeaders...)
	result.Outcome = delivery.Unknown
	result.ErrorType = "response_invalid"
	result.Err = errors.New("delivery success response was not valid JSON")
	return result
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return secondsDuration(seconds)
	}
	parsed, err := http.ParseTime(value)
	if err != nil || !parsed.After(now) {
		return 0
	}
	return parsed.Sub(now)
}

func secondsDuration(seconds int64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	// Avoid overflowing time.Duration on malicious provider/proxy input.
	const maxSeconds = int64((1<<63 - 1) / int64(time.Second))
	if seconds > maxSeconds {
		seconds = maxSeconds
	}
	return time.Duration(seconds) * time.Second
}

func sanitizeIdentifier(value string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > max {
		return ""
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("-_.:/", r) {
			continue
		}
		return ""
	}
	return value
}

func largerDelay(left, right time.Duration) time.Duration {
	if right > left {
		return right
	}
	return left
}
