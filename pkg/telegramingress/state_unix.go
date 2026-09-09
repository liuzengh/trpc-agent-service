//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package telegramingress

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func acquireStateLock(path string) (*os.File, error) {
	file, err := openLockFile(path)
	if err != nil {
		return nil, err
	}
	if unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		file.Close()
		return nil, ErrAlreadyLocked
	}
	return file, nil
}

func replaceCheckpoint(temporary, path string) error {
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
