package approval

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func TestMalformedDecisionsNeverChangeStateOrCreateAgentTasks(t *testing.T) {
	for _, template := range []string{"拒绝请回复：拒绝 ID", "@demo_bot 拒绝 ID", "请帮我取消审批 ID", "拒绝 ID extra", "取消", "算了"} {
		t.Run(template, func(t *testing.T) {
			ctx := context.Background()
			repo := NewMemoryRepository()
			journal := gateway.NewMemoryJournal()
			service, _ := NewService(repo, journal, nil)
			record, err := repo.Request(ctx, approvalFixture())
			if err != nil {
				t.Fatal(err)
			}
			input := gateway.ApprovalDecisionInput{
				Scope: runtimecontext.TutorialScope(), TenantID: record.TenantID, ChannelType: "http",
				ChannelBindingID: record.ChannelBindingID, UserID: record.UserID, SessionID: record.SessionID,
				ChatType: "direct", ExternalMessageID: "bad-decision", Text: strings.ReplaceAll(template, "ID", record.ApprovalID),
			}
			for i := 0; i < 2; i++ {
				if handled, err := service.HandleApprovalDecision(ctx, input); !handled || err != nil {
					t.Fatalf("handled=%t err=%v", handled, err)
				}
			}
			pending, err := repo.ListPendingByRequest(ctx, record.TenantID, record.RequestID)
			if err != nil || len(pending) != 1 || len(journal.Tasks()) != 0 {
				t.Fatal("invalid format changed state or created work")
			}
			out, err := journal.ClaimOutbound(ctx, "sender", 10, time.Minute)
			if err != nil || len(out) != 1 || !strings.Contains(out[0].Text, "没有更改审批状态") {
				t.Fatalf("feedback=%+v err=%v", out, err)
			}
			if strings.Contains(out[0].Text, "已取消") {
				t.Fatal("unverified cancellation confirmation")
			}
		})
	}
}

func TestOrdinaryChatIsNotAnApprovalCommand(t *testing.T) {
	service, _ := NewService(NewMemoryRepository(), gateway.NewMemoryJournal(), nil)
	for _, text := range []string{"你好", "今天几号", "我不同意这个观点", "同意你的观点", "取消订单流程是什么", "请介绍一下 Go"} {
		if handled, err := service.HandleApprovalDecision(context.Background(), gateway.ApprovalDecisionInput{Text: text}); handled || err != nil {
			t.Fatalf("ordinary chat intercepted: %q handled=%t err=%v", text, handled, err)
		}
	}
}
