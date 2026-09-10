package protocol_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestVerifierChecksOfficialEncryptedXMLContract(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := verificationMaterial()
	secret, _ := json.Marshal(material)
	plaintext := textPayload(material, "message-1", "hello")
	encrypted := encrypt(t, plaintext, material.EncodingAESKey, material.ReceiveID)
	request := callbackRequest(now, material, encrypted)
	verified, err := (protocol.Verifier{Now: func() time.Time { return now }}).Verify(context.Background(), request, secret)
	if err != nil || !bytes.Equal(verified.Body, plaintext) || verified.ProtocolIdentityDigest == "" ||
		verified.Headers["x-wecom-receive-id"] != material.ReceiveID || verified.Headers["x-wecom-agent-id"] != "218" {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}

	for name, mutate := range map[string]func(*channel.CallbackRequest){
		"bad signature": func(in *channel.CallbackRequest) { in.Query["msg_signature"] = "forged" },
		"stale timestamp": func(in *channel.CallbackRequest) {
			in.Query["timestamp"] = fmt.Sprint(now.Add(-6 * time.Minute).Unix())
		},
		"wrong wrapper corp": func(in *channel.CallbackRequest) {
			in.Body = encryptedEnvelope("other-corp", material.AgentID, encrypted)
			in.Query["msg_signature"] = protocol.Signature(material.Token, in.Query["timestamp"], in.Query["nonce"], encrypted)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.Query = clone(request.Query)
			mutate(&candidate)
			if _, err := (protocol.Verifier{Now: func() time.Time { return now }}).Verify(context.Background(), candidate, secret); err == nil {
				t.Fatal("invalid callback accepted")
			}
		})
	}
}

func TestVerifierChecksURLChallenge(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := verificationMaterial()
	secret, _ := json.Marshal(material)
	echo := encrypt(t, []byte("challenge-value"), material.EncodingAESKey, material.ReceiveID)
	timestamp, nonce := fmt.Sprint(now.Unix()), "1372623149"
	request := channel.CallbackRequest{Query: map[string]string{
		"timestamp": timestamp, "nonce": nonce, "echostr": echo,
		"msg_signature": protocol.Signature(material.Token, timestamp, nonce, echo),
	}}
	if !protocol.IsChallengeRequest(request) {
		t.Fatal("challenge not recognized")
	}
	verifier := protocol.Verifier{Now: func() time.Time { return now }}
	verified, err := verifier.VerifyChallenge(context.Background(), request, secret)
	if err != nil || string(verified.Body) != "challenge-value" ||
		verified.Headers["content-type"] != "text/plain; charset=utf-8" || verified.ProtocolIdentityDigest == "" {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}

	for name, mutate := range map[string]func(*channel.CallbackRequest){
		"bad signature": func(in *channel.CallbackRequest) { in.Query["msg_signature"] = "forged" },
		"stale": func(in *channel.CallbackRequest) {
			in.Query["timestamp"] = fmt.Sprint(now.Add(-6 * time.Minute).Unix())
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.Query = clone(request.Query)
			mutate(&candidate)
			if _, err := verifier.VerifyChallenge(context.Background(), candidate, secret); err == nil {
				t.Fatal("invalid challenge accepted")
			}
		})
	}

	wrongReceiveID := request
	wrongReceiveID.Query = clone(request.Query)
	wrongEcho := encrypt(t, []byte("challenge-value"), material.EncodingAESKey, "other-corp")
	wrongReceiveID.Query["echostr"] = wrongEcho
	wrongReceiveID.Query["msg_signature"] = protocol.Signature(material.Token, timestamp, nonce, wrongEcho)
	if _, err := verifier.VerifyChallenge(context.Background(), wrongReceiveID, secret); err == nil {
		t.Fatal("wrong receive ID accepted")
	}
}

func TestSignatureCompatibilityVector(t *testing.T) {
	const want = "6541f079a520db7df30e2ee4685e833822957fef"
	if got := protocol.Signature("callback-token", "1800000000", "1372623149", "ciphertext"); got != want {
		t.Fatalf("signature=%s want=%s", got, want)
	}
}

func TestVerifierRejectsReceiveIDAndPaddingTampering(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := verificationMaterial()
	secret, _ := json.Marshal(material)
	for name, encrypted := range map[string]string{
		"wrong receive id": encrypt(t, textPayload(material, "message-1", "hello"), material.EncodingAESKey, "other-corp"),
		"bad padding":      corruptPadding(t, encrypt(t, textPayload(material, "message-1", "hello"), material.EncodingAESKey, material.ReceiveID)),
	} {
		t.Run(name, func(t *testing.T) {
			request := callbackRequest(now, material, encrypted)
			if _, err := (protocol.Verifier{Now: func() time.Time { return now }}).Verify(context.Background(), request, secret); err == nil {
				t.Fatal("tampered callback accepted")
			}
		})
	}
}

func TestDecodeMessageMapsAgentTextToProviderEvent(t *testing.T) {
	material := verificationMaterial()
	event, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: textPayload(material, "message-1", "  hello 企业微信  "), Headers: map[string]string{
		"x-wecom-receive-id": material.ReceiveID, "x-wecom-agent-id": "218",
	}})
	if err != nil || event.Channel != "wecom" || event.ExternalAccountID != material.ReceiveID ||
		event.ExternalMessageID != "message-1" || event.ExternalUserID != "zhangsan" || event.ConversationType != "p2p" ||
		event.Text != "hello 企业微信" || !event.OccurredAt.Equal(time.Unix(1_800_000_000, 0).UTC()) {
		t.Fatalf("event=%#v err=%v", event, err)
	}

	invalid := textPayload(material, "message-1", "hello")
	invalid = bytes.Replace(invalid, []byte("<MsgType><![CDATA[text]]></MsgType>"), []byte("<MsgType><![CDATA[image]]></MsgType>"), 1)
	if _, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: invalid, Headers: map[string]string{
		"x-wecom-receive-id": material.ReceiveID, "x-wecom-agent-id": "218",
	}}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeMessageMapsAgentMediaToOpaqueProviderRef(t *testing.T) {
	material := verificationMaterial()
	for _, kind := range []string{"image", "file"} {
		t.Run(kind, func(t *testing.T) {
			body := mediaPayload(material, "message-1", kind, "media-id", 123)
			event, err := protocol.DecodeMessage(channel.VerifiedCallback{Body: body, Headers: map[string]string{
				"x-wecom-receive-id": material.ReceiveID, "x-wecom-agent-id": "218",
			}})
			if err != nil || event.MessageType != kind || event.Text != "" || len(event.MediaRefs) != 1 ||
				event.MediaRefs[0].ID != "media-id" || event.MediaRefs[0].MessageID != "message-1" || event.MediaRefs[0].Kind != kind || event.MediaRefs[0].Size != 123 {
				t.Fatalf("event=%#v err=%v", event, err)
			}
		})
	}
}

func verificationMaterial() protocol.VerificationMaterial {
	key := []byte("0123456789abcdef0123456789abcdef")
	return protocol.VerificationMaterial{Token: "callback-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(key), ReceiveID: "ww_corp", AgentID: 218}
}

func callbackRequest(now time.Time, material protocol.VerificationMaterial, encrypted string) channel.CallbackRequest {
	timestamp, nonce := fmt.Sprint(now.Unix()), "1372623149"
	return channel.CallbackRequest{Body: encryptedEnvelope(material.ReceiveID, material.AgentID, encrypted), ReceivedAt: now, Query: map[string]string{
		"timestamp": timestamp, "nonce": nonce, "msg_signature": protocol.Signature(material.Token, timestamp, nonce, encrypted),
	}}
}

func encryptedEnvelope(receiveID string, agentID int64, encrypted string) []byte {
	return []byte(fmt.Sprintf("<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID>%d</AgentID></xml>", receiveID, encrypted, agentID))
}

func textPayload(material protocol.VerificationMaterial, messageID, content string) []byte {
	type cdata struct {
		Value string `xml:",cdata"`
	}
	payload := struct {
		XMLName      xml.Name `xml:"xml"`
		ToUserName   cdata    `xml:"ToUserName"`
		FromUserName cdata    `xml:"FromUserName"`
		CreateTime   int64    `xml:"CreateTime"`
		MsgType      cdata    `xml:"MsgType"`
		Content      cdata    `xml:"Content"`
		MsgID        string   `xml:"MsgId"`
		AgentID      int64    `xml:"AgentID"`
	}{ToUserName: cdata{material.ReceiveID}, FromUserName: cdata{"zhangsan"}, CreateTime: 1_800_000_000,
		MsgType: cdata{"text"}, Content: cdata{content}, MsgID: messageID, AgentID: material.AgentID}
	encoded, _ := xml.Marshal(payload)
	return encoded
}

func mediaPayload(material protocol.VerificationMaterial, messageID, kind, mediaID string, size int64) []byte {
	type cdata struct {
		Value string `xml:",cdata"`
	}
	payload := struct {
		XMLName                  xml.Name `xml:"xml"`
		ToUserName, FromUserName cdata
		CreateTime               int64  `xml:"CreateTime"`
		MsgType                  cdata  `xml:"MsgType"`
		MediaID                  cdata  `xml:"MediaId"`
		FileSize                 int64  `xml:"FileSize"`
		MsgID                    string `xml:"MsgId"`
		AgentID                  int64  `xml:"AgentID"`
	}{ToUserName: cdata{material.ReceiveID}, FromUserName: cdata{"zhangsan"}, CreateTime: 1_800_000_000,
		MsgType: cdata{kind}, MediaID: cdata{mediaID}, FileSize: size, MsgID: messageID, AgentID: material.AgentID}
	encoded, _ := xml.Marshal(payload)
	return encoded
}

func encrypt(t *testing.T, message []byte, encodingAESKey, receiveID string) string {
	t.Helper()
	key, err := base64.RawStdEncoding.DecodeString(encodingAESKey)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := append([]byte("0123456789abcdef"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(plaintext[16:20], uint32(len(message)))
	plaintext = append(plaintext, message...)
	plaintext = append(plaintext, receiveID...)
	padding := 32 - len(plaintext)%32
	plaintext = append(plaintext, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext)
}

func corruptPadding(t *testing.T, encrypted string) string {
	t.Helper()
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	return base64.StdEncoding.EncodeToString(ciphertext)
}

func clone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
