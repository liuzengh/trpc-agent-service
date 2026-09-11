package domain

import "testing"

func TestEndpointProfileSwitchIsConnectionChange(t *testing.T) {
	a := fixtureAccount(t)
	b, changed, e := a.ChangeConfiguration(1, nil, nil, nil, testTime, "test")
	if e != nil || !changed || b.Config.EndpointProfile != "test" || b.ConnectionRevision != 2 || b.Revision != 2 {
		t.Fatal(b, e)
	}
	b.Enabled = true
	if _, _, e = b.ChangeConfiguration(2, nil, nil, nil, testTime, "official"); e == nil {
		t.Fatal("enabled switch accepted")
	}
	b.Enabled = false
	c, changed, e := b.ChangeConfiguration(2, nil, nil, nil, testTime, "official")
	if e != nil || !changed || c.Config.EndpointProfile != "" || c.ConnectionRevision != 3 {
		t.Fatal(c, e)
	}
	if _, _, e = c.ChangeConfiguration(3, nil, nil, nil, testTime, "http://arbitrary"); e == nil {
		t.Fatal("URL accepted")
	}
}
