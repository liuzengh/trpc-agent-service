package wecombot

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const defaultMaxFileBytes = int64(16 << 20)

func (c *Client) downloadMedia(ctx context.Context, mediaType string, media mediaContent) (channels.ReceivedFile, error) {
	rawURL := strings.TrimSpace(media.URL)
	if rawURL == "" {
		return channels.ReceivedFile{}, errors.New("wecombot: media URL is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return channels.ReceivedFile{}, errors.New("wecombot: media URL must be HTTPS")
	}
	client := c.config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	maxBytes := c.config.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxFileBytes
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return channels.ReceivedFile{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return channels.ReceivedFile{}, fmt.Errorf("wecombot: download media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return channels.ReceivedFile{}, fmt.Errorf("wecombot: media download HTTP status %d", resp.StatusCode)
	}
	encrypted, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+33))
	if err != nil {
		return channels.ReceivedFile{}, fmt.Errorf("wecombot: read media: %w", err)
	}
	data := encrypted
	if key := strings.TrimSpace(media.AESKey); key != "" {
		data, err = decryptMedia(encrypted, key)
		if err != nil {
			return channels.ReceivedFile{}, err
		}
	}
	if int64(len(data)) > maxBytes {
		return channels.ReceivedFile{}, fmt.Errorf("wecombot: media exceeds %d bytes", maxBytes)
	}
	name := responseFilename(resp.Header.Get("Content-Disposition"))
	if name == "" {
		name = path.Base(parsed.Path)
	}
	if name == "" || name == "." || name == "/" {
		name = defaultMediaName(mediaType)
	}
	mimeType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	return channels.ReceivedFile{Name: name, MimeType: mimeType, Data: data}, nil
}

// decryptMedia mirrors WeCom's official SDK: AES-256-CBC, IV=key[:16],
// AutoPadding disabled, then PKCS#7 padding (1..32) removed manually.
func decryptMedia(encrypted []byte, encodedKey string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != 32 {
		return nil, errors.New("wecombot: invalid media AES key")
	}
	if len(encrypted) == 0 || len(encrypted)%aes.BlockSize != 0 {
		return nil, errors.New("wecombot: invalid encrypted media length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("wecombot: construct media cipher: %w", err)
	}
	plain := make([]byte, len(encrypted))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plain, encrypted)
	padLen := int(plain[len(plain)-1])
	if padLen < 1 || padLen > 32 || padLen > len(plain) {
		return nil, errors.New("wecombot: invalid media padding")
	}
	for _, value := range plain[len(plain)-padLen:] {
		if int(value) != padLen {
			return nil, errors.New("wecombot: invalid media padding")
		}
	}
	return plain[:len(plain)-padLen], nil
}

func responseFilename(contentDisposition string) string {
	_, params, err := mime.ParseMediaType(contentDisposition)
	if err != nil {
		return ""
	}
	return path.Base(strings.ReplaceAll(params["filename"], "\\", "/"))
}

func defaultMediaName(mediaType string) string {
	switch mediaType {
	case "image":
		return "image.jpg"
	case "video":
		return "video"
	default:
		return "attachment"
	}
}
