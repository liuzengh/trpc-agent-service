package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestWeComUpgradePreservesReleasedSQL(t *testing.T) {
	for name, expected := range map[string]string{
		"0001_baseline.sql":               "4115087326828f4fdf62d6296aebed038bf95bbf8d5de03925b3c4ba368baf82",
		"0002_channel_preflights.sql":     "30bfc34fc49ff4af99619fec1e0339fa199aeab157e6eff3af25faba56dc266f",
		"0003_telegram_receive_modes.sql": "945d2787522de994a9f27e6e5c81730eea7468d63228f3e1c641c9b01e7c055c",
	} {
		t.Run(name, func(t *testing.T) {
			body, err := Files.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			if hex.EncodeToString(sum[:]) != expected {
				t.Fatal("released migration bytes changed")
			}
		})
	}
	if body, err := Files.ReadFile("0004_wecom_preflights.sql"); err != nil || len(body) == 0 {
		t.Fatal("WeCom upgrade is not embedded")
	}
}
