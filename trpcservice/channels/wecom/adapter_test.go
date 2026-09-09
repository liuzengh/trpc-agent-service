package wecom

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func newTestAdapter(t *testing.T) *Adapter {
	t.Helper()
	client, err := New(testConfig(t, newMockServer(t), testBinding()))
	require.NoError(t, err)
	adapter, err := NewAdapter(client)
	require.NoError(t, err)
	return adapter
}

// The identity the pipeline is told is the binding the Client was authorized
// for, and nothing about a frame can move it.
func TestNewAdapterTakesItsIdentityFromItsClient(t *testing.T) {
	binding := testBinding()
	adapter := newTestAdapter(t)
	require.Equal(t, channels.BindingIdentity{
		TenantID:   binding.TenantID,
		AgentAppID: binding.AgentAppID,
		BindingID:  binding.BindingID,
		Channel:    channels.ChannelWeCom,
	}, adapter.Identity())
	require.NoError(t, adapter.Identity().Validate())

	_, err := NewAdapter(nil)
	require.ErrorIs(t, err, ErrConfig)
}

// The reply limit is this platform's, and it has to be a limit the durable
// pipeline can store: an answer bounded to it is still a storable message.
func TestReplyTextLimitIsStorable(t *testing.T) {
	require.Equal(t, maxReplyTextBytes, newTestAdapter(t).ReplyTextLimit())
	require.LessOrEqual(t, maxReplyTextBytes, channels.MaxMessageTextBytes)
}

func TestDeliveryTargetIsUsableOnlyByItsOwnBinding(t *testing.T) {
	binding := testBinding()
	reply := ReplyTarget{generation: "gen-1", reqID: "req-1", streamID: "stream-1"}
	target, err := encodeTarget(binding, reply)
	require.NoError(t, err)
	decoded, err := decodeTarget(binding, target)
	require.NoError(t, err)
	require.Equal(t, reply, decoded)

	foreign := binding
	foreign.TenantID = "tenant-b"
	_, err = decodeTarget(foreign, target)
	require.ErrorIs(t, err, ErrTargetInvalid)

	otherBinding := binding
	otherBinding.BindingID = "binding-b"
	_, err = decodeTarget(otherBinding, target)
	require.ErrorIs(t, err, ErrTargetInvalid)

	wrongVersion := target
	wrongVersion.Version = targetVersion + 1
	_, err = decodeTarget(binding, wrongVersion)
	require.ErrorIs(t, err, ErrTargetInvalid)

	wrongChannel := target
	wrongChannel.Channel = channels.ChannelFeishu
	_, err = decodeTarget(binding, wrongChannel)
	require.ErrorIs(t, err, ErrTargetInvalid)

	// A target that is missing the connection it belongs to cannot be answered
	// on any connection.
	empty, err := encodeTarget(binding, ReplyTarget{reqID: "req-1", streamID: "stream-1"})
	require.NoError(t, err)
	_, err = decodeTarget(binding, empty)
	require.ErrorIs(t, err, ErrTargetInvalid)
}

// What this protocol reports, in the closed vocabulary the pipeline records.
// Anything this package has not classified is unknown rather than assumed
// delivered or assumed refused, because the pipeline treats unknown as a
// duplicate risk and stops, and treats the other two as settled.
func TestSendClassificationFailsClosed(t *testing.T) {
	require.Equal(t, channels.Delivered, deliveryOutcome(nil))
	for _, err := range []error{ErrReplyTargetExpired, ErrNotConnected, ErrReplyAlreadySent} {
		require.Equal(t, channels.DeliveryTargetStale, deliveryOutcome(err))
	}
	for _, err := range []error{ErrReplyRejected, ErrTextTooLong, ErrTextInvalid} {
		require.Equal(t, channels.DeliveryRejected, deliveryOutcome(err))
	}
	for _, err := range []error{
		ErrReplyOutcomeUnknown, errAckTimeout, context.DeadlineExceeded,
	} {
		require.Equal(t, channels.DeliveryUnknown, deliveryOutcome(err))
	}
}

// A reply address this binding cannot read is reported as an address that is
// gone, and nothing is put on the wire to find out.
func TestSendRefusesAnUnreadableTarget(t *testing.T) {
	adapter := newTestAdapter(t)
	for _, target := range []channels.DeliveryTarget{
		{},
		{Channel: channels.ChannelWeCom, Version: targetVersion, Payload: []byte(`{}`)},
	} {
		report := adapter.Send(
			context.Background(), target, channels.OutboundMessage{Text: replyMarker})
		require.Equal(t, channels.DeliveryTargetStale, report.Outcome)
		require.Empty(t, report.ExternalMessageID)
		require.Equal(t, channels.SendPermanent, report.SendResult().Outcome)
	}
}
