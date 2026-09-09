package wecombot

import (
	"os"

	"golang.org/x/sys/windows"
)

func acquireStateLock(path string) (*os.File, error) {
	file, err := openLockFile(path)
	if err != nil {
		return nil, err
	}
	var overlapped windows.Overlapped
	if windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped) != nil {
		file.Close()
		return nil, ErrLocked
	}
	return file, nil
}

func replaceCheckpoint(temporary, path string) error {
	source, err := windows.UTF16PtrFromString(temporary)
	if err != nil {
		return err
	}
	destination, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(source, destination, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
