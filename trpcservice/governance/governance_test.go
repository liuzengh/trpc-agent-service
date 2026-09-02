package governance

import (
	"context"
	"errors"
	"testing"
)

func TestRedactMasksSensitiveData(t *testing.T) {
	cases := map[string]string{
		"手机号 13812345678 联系我":         "手机号 138***78 联系我",
		"身份证 110101199001011234 有效":   "身份证 110***34 有效",
		"卡号 6222021234567890123 已绑定": "卡号 622***23 已绑定",
		"邮箱 foo@example.com 请联系":       "邮箱 foo***om 请联系",
	}
	for in, want := range cases {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactLeavesCleanText(t *testing.T) {
	clean := "你好，帮我查一下商品价格。"
	if got := Redact(clean); got != clean {
		t.Errorf("Redact changed clean text: %q -> %q", clean, got)
	}
}

func TestBudgetExceeded(t *testing.T) {
	ctx := context.Background()

	b := &Budget{QuotaTokens: 1000, Usage: func(_ context.Context, _ string) (int64, error) {
		return 999, nil
	}}
	ex, err := b.Exceeded(ctx, "t1")
	if err != nil || ex {
		t.Errorf("usage 999 under quota 1000: exceeded=%v err=%v", ex, err)
	}

	b2 := &Budget{QuotaTokens: 1000, Usage: func(_ context.Context, _ string) (int64, error) {
		return 1000, nil
	}}
	ex, err = b2.Exceeded(ctx, "t1")
	if err != nil || !ex {
		t.Errorf("usage 1000 at quota 1000: exceeded=%v err=%v, want true", ex, err)
	}

	// nil usage / zero quota disables the check
	var nb *Budget
	if ex, _ := nb.Exceeded(ctx, "t1"); ex {
		t.Error("nil budget should never be exceeded")
	}
}

func TestBudgetUsageErrorPropagates(t *testing.T) {
	b := &Budget{QuotaTokens: 10, Usage: func(_ context.Context, _ string) (int64, error) {
		return 0, errors.New("boom")
	}}
	if _, err := b.Exceeded(context.Background(), "t1"); err == nil {
		t.Error("usage error should propagate")
	}
}

func TestPermissionCheck(t *testing.T) {
	ctx := context.Background()

	// nil permission allows all (default-open)
	var np *Permission
	if !np.Check(ctx, "t1", "u1") {
		t.Error("nil permission should allow")
	}

	p := &Permission{Allowed: func(_ context.Context, _, user string) bool {
		return user == "alice"
	}}
	if !p.Check(ctx, "t1", "alice") {
		t.Error("alice should be allowed")
	}
	if p.Check(ctx, "t1", "bob") {
		t.Error("bob should be denied")
	}
}
