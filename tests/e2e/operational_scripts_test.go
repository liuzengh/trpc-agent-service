package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOperationalScriptsRejectShellExpressionsWithoutExecutingThem(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	temporaryDirectory := t.TempDir()

	for _, testCase := range []struct {
		name        string
		script      string
		environment func(marker string) []string
	}{
		{
			name:   "backup restore",
			script: filepath.Join(root, "scripts", "backup-restore-smoke.sh"),
			environment: func(marker string) []string {
				return []string{
					"BACKUP_COMMAND=touch " + marker,
					"RESTORE_COMMAND=/bin/true",
					"VERIFY_COMMAND=/bin/true",
				}
			},
		},
		{
			name:   "secret rotation",
			script: filepath.Join(root, "scripts", "secret-rotation-smoke.sh"),
			environment: func(marker string) []string {
				return []string{
					"KUBE_CONTEXT=not-used",
					"SECRET_ROTATION_COMMAND=touch " + marker,
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			marker := filepath.Join(temporaryDirectory, "unsafe-marker")
			command := exec.Command("bash", testCase.script)
			command.Env = append(os.Environ(), testCase.environment(marker)...)
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("script accepted a shell expression: %s", output)
			}
			if _, err := os.Stat(marker); err == nil {
				t.Fatal("script executed the rejected shell expression")
			} else if !os.IsNotExist(err) {
				t.Fatalf("inspect unsafe command marker: %v", err)
			}
		})
	}
}
