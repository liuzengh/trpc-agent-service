package governance

import (
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
