// Package attachments implements the production IM media-to-artifact path.
package attachments

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

const productionInboundMediaLimit int64 = 32 << 20

const (
	wecomMediaRequestTimeout           = 30 * time.Second
	wecomMediaPort                     = "443"
	inboundArtifactUploadRenewInterval = time.Minute
)

// NewIngestor creates the production media ingestor. Media is downloaded only
// after binding authorization, stored in COS, and represented downstream by an
// ArtifactRef.
func NewIngestor(
	store *postgres.Store,
	secrets platformsecret.SecretProvider,
	resolver *artifactcos.Resolver,
) (channels.AttachmentIngestor, error) {
	if store == nil || secrets == nil || resolver == nil {
		return nil, errors.New("attachment ingestor dependencies are required")
	}
	return channels.NewArtifactIngestor(
		mediaDownloader{store: store, secrets: secrets},
		inboundArtifactWriter{store: store, resolver: resolver},
	)
}

// mediaDownloader resolves media only through the verified binding selected
// by ChannelInput. WeCom uses its provider URL; Feishu uses the official
// message-resource API and the binding's app credentials.
type mediaDownloader struct {
	store   *postgres.Store
	secrets platformsecret.SecretProvider
}

func (d mediaDownloader) Download(
	ctx context.Context,
	input channels.ChannelInput,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if d.store == nil || d.secrets == nil {
		return channels.DownloadedMedia{}, errors.New("production media downloader is not initialized")
	}
	binding, err := d.store.ResolveBinding(ctx, input.TenantID, input.AppID, input.BindingID)
	if err != nil {
		return channels.DownloadedMedia{}, err
	}
	if binding.Status != channels.BindingActive || binding.BindingRevision != input.BindingRevision ||
		binding.Channel != input.Channel {
		return channels.DownloadedMedia{}, errors.New("media binding authorization changed")
	}
	switch input.Channel {
	case channels.ChannelWeCom:
		return d.downloadWeCom(ctx, media)
	case channels.ChannelFeishu:
		client, err := feishu.NewOutboundClient(ctx, d.secrets, binding.Snapshot())
		if err != nil {
			return channels.DownloadedMedia{}, err
		}
		return client.DownloadMediaForMessage(ctx, input.ExternalMessageID, media)
	default:
		return channels.DownloadedMedia{}, fmt.Errorf("unsupported media channel %q", input.Channel)
	}
}

func (d mediaDownloader) downloadWeCom(
	ctx context.Context,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if err := media.Validate(); err != nil {
		return channels.DownloadedMedia{}, err
	}
	parsed, err := url.Parse(media.Reference)
	if err != nil || !isValidWeComMediaURL(parsed) {
		return channels.DownloadedMedia{}, errors.New("wecom media url is invalid")
	}
	client := newWeComMediaClient()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return channels.DownloadedMedia{}, errors.New("create wecom media request")
	}
	response, err := client.Do(request)
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("download wecom media: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return channels.DownloadedMedia{}, fmt.Errorf("wecom media returned status %d", response.StatusCode)
	}
	if response.ContentLength > productionInboundMediaLimit {
		return channels.DownloadedMedia{}, errors.New("wecom media exceeds size limit")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, productionInboundMediaLimit+1))
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("read wecom media: %w", err)
	}
	if int64(len(data)) > productionInboundMediaLimit {
		return channels.DownloadedMedia{}, errors.New("wecom media exceeds size limit")
	}
	if media.DecryptionKey != "" {
		data, err = decryptWeComMedia(data, media.DecryptionKey)
		if err != nil {
			return channels.DownloadedMedia{}, fmt.Errorf("decrypt wecom media: %w", err)
		}
	}
	filename := filenameFromContentDisposition(response.Header.Get("Content-Disposition"))
	if filename == "" {
		filename = path.Base(parsed.Path)
	}
	if filename == "." || filename == "/" || filename == "" {
		filename = "attachment"
	}
	// Do not trust the provider HTTP header for capability classification. A
	// wrong header must not turn arbitrary bytes into an image/audio part.
	mimeType := channels.DetectMediaMIMEType(filename, data)
	if _, _, err := mime.ParseMediaType(mimeType); err != nil {
		mimeType = "application/octet-stream"
	}
	return channels.DownloadedMedia{Filename: filename, MIMEType: mimeType, Data: data}, nil
}

func filenameFromContentDisposition(value string) string {
	_, params, err := mime.ParseMediaType(value)
	if err != nil {
		return ""
	}
	filename := strings.TrimSpace(params["filename"])
	if filename == "" {
		return ""
	}
	filename = path.Base(strings.ReplaceAll(filename, "\\", "/"))
	if filename == "." || filename == "/" {
		return ""
	}
	return filename
}

// decryptWeComMedia follows the official AI Bot SDK: the aeskey is Base64
// encoded, its first 16 decoded bytes are the CBC IV, and the payload uses
// PKCS#7 padding with a 32-byte padding block.
func decryptWeComMedia(encrypted []byte, encodedKey string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		key, err = base64.RawStdEncoding.DecodeString(encodedKey)
	}
	if err != nil || len(key) != 32 {
		return nil, errors.New("wecom media aeskey is invalid")
	}
	if len(encrypted) == 0 || len(encrypted)%aes.BlockSize != 0 {
		return nil, errors.New("wecom encrypted media length is invalid")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("create wecom media cipher")
	}
	decrypted := make([]byte, len(encrypted))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(decrypted, encrypted)
	padding := int(decrypted[len(decrypted)-1])
	if padding < 1 || padding > 32 || padding > len(decrypted) {
		return nil, errors.New("wecom media padding is invalid")
	}
	for _, value := range decrypted[len(decrypted)-padding:] {
		if int(value) != padding {
			return nil, errors.New("wecom media padding is invalid")
		}
	}
	return decrypted[:len(decrypted)-padding], nil
}

func isValidWeComMediaURL(value *url.URL) bool {
	if value == nil || value.Scheme != "https" || value.Host == "" || value.User != nil || value.Fragment != "" || value.Port() != "" {
		return false
	}
	return value.Hostname() != ""
}

func newWeComMediaClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialTLSContext = nil
	transport.DialContext = dialPublicWeComHost
	return &http.Client{
		Transport: transport,
		Timeout:   wecomMediaRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func dialPublicWeComHost(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port != wecomMediaPort {
		return nil, errors.New("wecom media address is not allowed")
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, errors.New("resolve wecom media host")
	}
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	for _, resolved := range addresses {
		ip := resolved.IP
		if !isPublicInternetIP(ip) {
			continue
		}
		if network == "tcp4" && ip.To4() == nil {
			continue
		}
		if network == "tcp6" && ip.To4() != nil {
			continue
		}
		connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return connection, nil
		}
	}
	return nil, errors.New("wecom media host has no public address")
}

func isPublicInternetIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return false
		}
	}
	return ip.IsGlobalUnicast() && !ip.IsPrivate()
}

type inboundArtifactWriter struct {
	store    *postgres.Store
	resolver *artifactcos.Resolver
}

func (w inboundArtifactWriter) WriteInboundArtifact(
	ctx context.Context,
	input channels.InboundArtifact,
) (string, bool, error) {
	if w.store == nil || w.resolver == nil {
		return "", false, errors.New("production artifact writer is not initialized")
	}
	if err := input.Validate(); err != nil {
		return "", false, err
	}
	if input.ConfigVersion == "" {
		return "", false, errors.New("inbound artifact config version is required")
	}
	config, err := w.store.ResolveAppConfig(ctx, input.TenantID, input.AppID, input.ConfigVersion)
	if err != nil {
		return "", false, err
	}
	ref := config.BackendConfig.Artifact
	if ref.IsZero() {
		return "", false, errors.New("artifact backend is required for inbound media")
	}
	objectStore, err := w.resolver.ResolveInboundStore(
		ctx,
		tenant.Scope{TenantID: input.TenantID, AppID: input.AppID},
		input.ConfigVersion,
		ref,
	)
	if err != nil {
		return "", false, err
	}
	objectID := uuid.NewString()
	artifactName := "inbound/" + objectID
	artifactRef := "artifact://" + artifactName + "@0"
	objectKey, err := objectStore.ObjectKey(objectID)
	if err != nil {
		return "", false, err
	}
	stagedInput := postgres.StagedInboundArtifact{
		TenantID:          input.TenantID,
		AppID:             input.AppID,
		BindingID:         input.BindingID,
		ExternalMessageID: input.ExternalMessageID,
		ItemNo:            input.ItemNo,
		ArtifactRef:       artifactRef,
		ConfigVersion:     input.ConfigVersion,
		Filename:          artifactName,
		ObjectKey:         objectKey,
		MIMEType:          input.MIMEType,
		Size:              int64(len(input.Data)),
	}
	reserved, err := w.store.ReserveInboundArtifact(ctx, stagedInput)
	if err != nil {
		return "", false, err
	}
	if !reserved.Created {
		switch reserved.Status {
		case "PENDING", "ATTACHED":
			return reserved.ArtifactRef, false, nil
		case "UPLOADING":
			return "", false, errors.New("inbound artifact upload is already in progress")
		default:
			return "", false, fmt.Errorf("inbound artifact status %q cannot be reused", reserved.Status)
		}
	}
	objectKey, err = w.putInboundArtifact(ctx, objectStore, objectID, &frameworkartifact.Artifact{
		Data:     input.Data,
		MimeType: input.MIMEType,
		Name:     input.Filename,
	}, reserved)
	if err != nil {
		_, _, cleanupErr := w.store.MarkInboundArtifactDeleted(context.WithoutCancel(ctx), reserved)
		if objectKey != "" {
			cleanupErr = errors.Join(cleanupErr, objectStore.Delete(context.WithoutCancel(ctx), objectKey))
		}
		return "", false, errors.Join(err, cleanupErr)
	}
	if objectKey != reserved.ObjectKey {
		_, _, cleanupErr := w.store.MarkInboundArtifactDeleted(context.WithoutCancel(ctx), reserved)
		cleanupErr = errors.Join(cleanupErr, objectStore.Delete(context.WithoutCancel(ctx), objectKey))
		return "", false, errors.Join(errors.New("inbound object key changed during upload"), cleanupErr)
	}
	staged, err := w.store.FinalizeInboundArtifactUpload(ctx, reserved)
	if err != nil {
		return "", false, errors.Join(err, objectStore.Delete(context.WithoutCancel(ctx), objectKey))
	}
	return staged.ArtifactRef, true, nil
}

// putInboundArtifact keeps the durable upload reservation alive while the
// object store may be slow. A renewal failure cancels the upload; the caller
// then compensates the durable reservation without letting an unowned upload
// reach PENDING.
func (w inboundArtifactWriter) putInboundArtifact(
	ctx context.Context,
	objectStore *artifactcos.InboundObjectStore,
	objectID string,
	value *frameworkartifact.Artifact,
	reserved postgres.StagedInboundArtifact,
) (string, error) {
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopRenewal := make(chan struct{})
	renewalResult := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(inboundArtifactUploadRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				renewalResult <- nil
				return
			case <-uploadCtx.Done():
				renewalResult <- uploadCtx.Err()
				return
			case <-ticker.C:
				if err := w.store.RenewInboundArtifactUpload(uploadCtx, reserved); err != nil {
					cancel()
					renewalResult <- err
					return
				}
			}
		}
	}()

	objectKey, putErr := objectStore.Put(uploadCtx, objectID, value)
	close(stopRenewal)
	renewalErr := <-renewalResult
	if renewalErr != nil {
		renewalErr = fmt.Errorf("renew inbound artifact upload: %w", renewalErr)
		if putErr != nil {
			return objectKey, errors.Join(putErr, renewalErr)
		}
		return objectKey, renewalErr
	}
	return objectKey, putErr
}

// DeleteInboundArtifact compensates a pre-admission upload. The durable stage
// is first moved out of PENDING, preventing a concurrent admission from
// attaching the object after it has been selected for deletion. ATTACHED
// stages are never deleted here.
func (w inboundArtifactWriter) DeleteInboundArtifact(
	ctx context.Context,
	input channels.InboundArtifact,
	artifactRef string,
) error {
	if w.store == nil || w.resolver == nil {
		return errors.New("production artifact writer is not initialized")
	}
	if input.ConfigVersion == "" || artifactRef == "" {
		return errors.New("inbound artifact compensation scope is required")
	}
	staged, shouldDelete, err := w.store.MarkInboundArtifactDeleted(ctx, postgres.StagedInboundArtifact{
		TenantID: input.TenantID, AppID: input.AppID, BindingID: input.BindingID,
		ExternalMessageID: input.ExternalMessageID, ItemNo: input.ItemNo,
		ArtifactRef: artifactRef, ConfigVersion: input.ConfigVersion,
	})
	if err != nil || !shouldDelete {
		return err
	}
	config, err := w.store.ResolveAppConfig(ctx, input.TenantID, input.AppID, staged.ConfigVersion)
	if err != nil {
		return err
	}
	if config.BackendConfig.Artifact.IsZero() {
		return errors.New("artifact backend is required for inbound media compensation")
	}
	return w.resolver.DeleteExactObject(
		ctx,
		tenant.Scope{TenantID: input.TenantID, AppID: input.AppID},
		staged.ConfigVersion,
		config.BackendConfig.Artifact,
		staged.ObjectKey,
	)
}

var _ channels.ArtifactCompensator = inboundArtifactWriter{}
