package identity

import "testing"

func TestNormalizeMemberPageRequestBoundsAndTrims(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   MemberPageRequest
		want MemberPageRequest
	}{
		{name: "default", in: MemberPageRequest{Query: "  alice  "}, want: MemberPageRequest{Limit: 50, Query: "alice"}},
		{name: "preserve", in: MemberPageRequest{Limit: 20, Query: " bob "}, want: MemberPageRequest{Limit: 20, Query: "bob"}},
		{name: "cap", in: MemberPageRequest{Limit: 1000, Query: " carol "}, want: MemberPageRequest{Limit: 200, Query: "carol"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeMemberPageRequest(test.in)
			if got.Limit != test.want.Limit || got.Query != test.want.Query {
				t.Fatalf("normalizeMemberPageRequest() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestMemberAfterProjectsCursor(t *testing.T) {
	t.Parallel()
	if enabled, name, userID := memberAfter(nil); enabled || name != "" || userID != "" {
		t.Fatalf("memberAfter(nil) = %v, %q, %q", enabled, name, userID)
	}
	cursor := &MemberCursor{DisplayName: "客服成员", PlatformUserID: "user-1"}
	if enabled, name, userID := memberAfter(cursor); !enabled || name != cursor.DisplayName || userID != cursor.PlatformUserID {
		t.Fatalf("memberAfter(cursor) = %v, %q, %q", enabled, name, userID)
	}
}
