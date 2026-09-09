package tenant

import (
	"errors"
	"testing"
)

func TestTenantServiceUnavailableNeverPanics(t *testing.T) {
	var nilService *TenantService
	typedNilRepo := (*mockRepository)(nil)
	service, err := NewServiceWithKeyRing(typedNilRepo, "v1", map[string]string{"v1": "master-key"})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		call func(*TenantService) error
	}{
		{name: "nil create", call: func(s *TenantService) error {
			_, err := s.CreateTenant(nil, "acme", TenantConfig{})
			return err
		}},
		{name: "nil get", call: func(s *TenantService) error {
			_, err := s.GetTenant(nil, "tenant-a")
			return err
		}},
		{name: "nil webhook", call: func(s *TenantService) error {
			_, err := s.GetTenantByWebhookToken(nil, "key")
			return err
		}},
		{name: "nil update", call: func(s *TenantService) error {
			return s.UpdateTenant(nil, nil)
		}},
		{name: "nil delete", call: func(s *TenantService) error {
			return s.DeleteTenant(nil, "tenant-a")
		}},
		{name: "nil list", call: func(s *TenantService) error {
			_, err := s.ListTenants(nil)
			return err
		}},
		{name: "nil scoped list", call: func(s *TenantService) error {
			_, err := s.ListTenantsForIDs(nil, []string{"tenant-a"})
			return err
		}},
		{name: "nil rotate", call: func(s *TenantService) error {
			return s.RotateEncryptionKey(nil, "v2", "new-master")
		}},
		{name: "nil close", call: func(s *TenantService) error {
			return s.Close()
		}},
	}
	for _, test := range cases {
		t.Run("nil receiver/"+test.name, func(t *testing.T) {
			if err := test.call(nilService); !errors.Is(err, ErrTenantServiceUnavailable) {
				t.Fatalf("error=%v, want ErrTenantServiceUnavailable", err)
			}
		})
		t.Run("typed nil repository/"+test.name, func(t *testing.T) {
			if err := test.call(service); !errors.Is(err, ErrTenantServiceUnavailable) {
				t.Fatalf("error=%v, want ErrTenantServiceUnavailable", err)
			}
		})
	}
}

func TestTenantServiceClosePurgesKeysBeforeReportingUnavailable(t *testing.T) {
	service, err := NewServiceWithKeyRing(nil, "v1", map[string]string{"v1": "master-key"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); !errors.Is(err, ErrTenantServiceUnavailable) {
		t.Fatalf("Close error=%v, want ErrTenantServiceUnavailable", err)
	}
	if len(service.encryptKey) != 0 || len(service.keyRing) != 0 || service.keyID != "" {
		t.Fatalf("Close retained key material: keyID=%q encryptKey=%d keyRing=%d", service.keyID, len(service.encryptKey), len(service.keyRing))
	}
}
