package message

import (
	"testing"
	"time"
)

func TestTaskV2RejectsV1AndRequiresConversationIdentity(t *testing.T) {
	task := validTaskV2()
	task.SchemaVersion = 1
	if err := task.Validate(); err == nil {
		t.Fatal("v1 task was accepted")
	}
	for _, mutate := range []func(*ExecutionTask){
		func(current *ExecutionTask) { current.ExternalAccountID = "" },
		func(current *ExecutionTask) { current.ActorUserID = "" },
		func(current *ExecutionTask) { current.ConversationID = "" },
		func(current *ExecutionTask) { current.ConversationType = "" },
	} {
		current := validTaskV2()
		mutate(&current)
		current.PayloadDigest = current.CanonicalDigest()
		if err := current.Validate(); err == nil {
			t.Fatalf("incomplete v2 task was accepted: %#v", current)
		}
	}
}

func TestTaskDigestCoversV2IdentityAndDeliveryFields(t *testing.T) {
	original := validTaskV2()
	mutations := []func(*ExecutionTask){
		func(current *ExecutionTask) { current.Channel = "wecom_aibot" },
		func(current *ExecutionTask) { current.ChannelBindingID = "binding-b" },
		func(current *ExecutionTask) { current.ExternalAccountID = "account-b" },
		func(current *ExecutionTask) { current.PlatformMessageID = "message-b" },
		func(current *ExecutionTask) { current.RunnerUserID = "runner-b" },
		func(current *ExecutionTask) { current.SessionID = "session-b" },
		func(current *ExecutionTask) { current.ActorUserID = "actor-b" },
		func(current *ExecutionTask) { current.ConversationID = "conversation-b" },
		func(current *ExecutionTask) { current.ConversationType = ConversationGroup },
		func(current *ExecutionTask) { current.ReplyToMessageID = "reply-b" },
		func(current *ExecutionTask) { current.PlatformRequestID = "platform-request-b" },
	}
	for index, mutate := range mutations {
		current := original
		mutate(&current)
		if current.ValidDigest() {
			t.Fatalf("mutation %d did not invalidate digest", index)
		}
	}
}

func TestDeliveryTargetComesFromExecutionTask(t *testing.T) {
	task := validTaskV2()
	target := task.DeliveryTarget()
	if !target.Valid() || target.Channel != task.Channel || target.ChannelBindingID != task.ChannelBindingID || target.ExternalAccountID != task.ExternalAccountID || target.PlatformRequestID != task.PlatformRequestID {
		t.Fatalf("delivery target = %#v", target)
	}
	reply := target.Apply(OutboundMessage{Channel: "untrusted", BindingID: "untrusted", Text: "answer"})
	if reply.Channel != task.Channel || reply.BindingID != task.ChannelBindingID || reply.ConversationID != task.ConversationID {
		t.Fatalf("trusted reply projection = %#v", reply)
	}
}

func TestCanonicalDigestV2RequiresTraceParent(t *testing.T) {
	task := validTaskV2()
	legacy := task.CanonicalDigest()
	task.DigestVersion = 2
	if task.CanonicalDigest() != legacy {
		t.Fatal("v2 digest changed without a trace parent")
	}
	task.TraceParent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
	if task.CanonicalDigest() == legacy {
		t.Fatal("trace parent was not included in v2 digest")
	}
}

func TestBusinessDigestIgnoresTraceMetadata(t *testing.T) {
	first := validTaskV2()
	first.DigestVersion = 2
	first.TraceParent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
	second := first
	second.TraceParent = "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"
	if first.BusinessDigest() != second.BusinessDigest() {
		t.Fatal("business digest changed with trace metadata")
	}
	second.Text = "changed"
	if first.BusinessDigest() == second.BusinessDigest() {
		t.Fatal("business digest ignored payload change")
	}
}

func validTaskV2() ExecutionTask {
	task := ExecutionTask{
		SchemaVersion: TaskSchemaVersion, TaskID: "task-a", Channel: "telegram", ChannelBindingID: "binding-a", ExternalAccountID: "account-a",
		TenantID: "tenant-a", AgentAppID: "agent-a", ConfigVersion: "v1", RunnerUserID: "runner-a", SessionID: "session-a",
		PlatformMessageID: "message-a", ActorUserID: "actor-a", ConversationID: "conversation-a", ConversationType: ConversationDirect,
		ReplyToMessageID: "reply-a", PlatformRequestID: "platform-request-a", Text: "hello", RequestID: "request-a", TraceID: "trace-a",
		ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}
