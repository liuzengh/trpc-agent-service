package channelv1

import "testing"

func TestReceiveModeDigestIgnoresOnlyIrrelevantOrigin(t *testing.T) {
	const scope = "pool"
	const epoch = "11111111-1111-4111-8111-111111111111"
	origin := "https://gateway.example.com"
	a, err := PreflightEffectiveConfigDigest(scope, epoch, "long_polling", 3, nil, "PUBLIC_ORIGIN_INVALID")
	if err != nil {
		t.Fatal(err)
	}
	b, err := PreflightEffectiveConfigDigest(scope, epoch, "long_polling", 3, &origin, "PUBLIC_ORIGIN_STATIC_VALID")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a != "sha256:ff0c679e5f1f8f6220d308fc33f5bb250ccf2b2b5a145c518d1ae7f0da489c1c" {
		t.Fatal("polling digest depends on origin")
	}
	c, _ := PreflightEffectiveConfigDigest(scope, epoch, "webhook", 3, &origin, "PUBLIC_ORIGIN_STATIC_VALID")
	if c != "sha256:46103e70d4ebf489b95a2eb1c1d972afb30b8e2d47646f5ba4060fa54397d5f5" || c == a {
		t.Fatal("mode is not bound")
	}
	d, _ := PreflightEffectiveConfigDigest(scope, epoch, "long_polling", 4, nil, "PUBLIC_ORIGIN_INVALID")
	if d == a {
		t.Fatal("connection revision is not bound")
	}
}
