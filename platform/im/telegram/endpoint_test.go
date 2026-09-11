package telegram

import "testing"

func TestEndpointSelection(t *testing.T) {
	for profile, want := range map[string]string{"": "https://api.telegram.org", "official": "https://api.telegram.org", "test": TestAPIURL} {
		got, e := Endpoint(profile, "")
		if e != nil || got != want {
			t.Fatal(got, e)
		}
	}
	if _, e := Endpoint("http://arbitrary", ""); e == nil {
		t.Fatal("arbitrary endpoint")
	}
	c, e := New(Options{Token: "123:synthetic_token", BaseURL: TestAPIURL})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	if _, e = New(Options{Token: "123:synthetic_token", BaseURL: "http://another-service:8080"}); e == nil {
		t.Fatal("arbitrary HTTP allowed")
	}
}
