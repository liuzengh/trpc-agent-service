package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func clearControlPlaneEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CONTROLPLANE_MODE", "")
	t.Setenv("CONTROLPLANE_MYSQL_DSN", "")
}

func TestControlPlaneDefaultsToLegacy(t *testing.T) {
	clearControlPlaneEnv(t)
	cfg, err := Load(writeConfig(t, wecomYAML)) // no control_plane section at all
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ControlPlane.Mode != ControlPlaneLegacy || cfg.ControlPlane.MySQLDSN != "" {
		t.Fatalf("control_plane defaults = %+v", cfg.ControlPlane)
	}
}

func TestControlPlaneMySQLNeedsADSN(t *testing.T) {
	clearControlPlaneEnv(t)
	doc := "control_plane:\n  mode: mysql\n" + wecomYAML
	if _, err := Load(writeConfig(t, doc)); err == nil {
		t.Fatal("mode: mysql with no dsn must fail validation")
	} else if !strings.Contains(err.Error(), "control_plane.mysql_dsn") {
		t.Fatalf("error must name the missing dsn, got: %v", err)
	}

	doc = "control_plane:\n  mode: mysql\n  mysql_dsn: 'tas:taspw@tcp(127.0.0.1:3306)/tas'\n" + wecomYAML
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ControlPlane.Mode != ControlPlaneMySQL ||
		cfg.ControlPlane.MySQLDSN != "tas:taspw@tcp(127.0.0.1:3306)/tas" {
		t.Fatalf("control_plane = %+v", cfg.ControlPlane)
	}
}

func TestControlPlaneRejectsAnUnknownMode(t *testing.T) {
	clearControlPlaneEnv(t)
	doc := "control_plane:\n  mode: postgres\n  mysql_dsn: x\n" + wecomYAML
	if _, err := Load(writeConfig(t, doc)); err == nil {
		t.Fatal("an unknown mode must fail validation")
	}
}

func TestControlPlaneEnvOverride(t *testing.T) {
	t.Setenv("CONTROLPLANE_MODE", "mysql")
	t.Setenv("CONTROLPLANE_MYSQL_DSN", "envuser:envpass@tcp(db:3306)/envdb")
	cfg, err := Load(writeConfig(t, wecomYAML)) // file says nothing about control_plane
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ControlPlane.Mode != ControlPlaneMySQL ||
		cfg.ControlPlane.MySQLDSN != "envuser:envpass@tcp(db:3306)/envdb" {
		t.Fatalf("env override not applied: %+v", cfg.ControlPlane)
	}
}

func TestSaveControlPlaneRoundTrip(t *testing.T) {
	clearControlPlaneEnv(t)
	doc := "control_plane:\n  mode: mysql\n  mysql_dsn: 'u:p@tcp(h:3306)/d'\n" + wecomYAML
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "saved.yaml")
	if err := Save(dst, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, err := Load(dst)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again.ControlPlane != cfg.ControlPlane {
		t.Fatalf("control_plane lost on round-trip: %+v vs %+v", again.ControlPlane, cfg.ControlPlane)
	}
}

// TestSaveLegacyControlPlaneOmitsTheSection is the backward-compatibility
// half of the change: a config that never mentions control_plane must not
// start carrying one after an unrelated Save, or every existing deployment's
// file would diff on the next admin write.
func TestSaveLegacyControlPlaneOmitsTheSection(t *testing.T) {
	clearControlPlaneEnv(t)
	cfg, err := Load(writeConfig(t, wecomYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	data, err := Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "control_plane") {
		t.Fatalf("legacy config gained a control_plane section:\n%s", data)
	}
}
