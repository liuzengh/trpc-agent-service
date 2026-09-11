package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// LoadDotEnv loads environment variables from path without replacing values
// that are already present in the process environment. A missing default file
// is treated as optional; malformed or unreadable files return an error.
func LoadDotEnv(path string) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, nil
	}
	if err := godotenv.Load(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("load dotenv file %q: %w", path, err)
	}
	return true, nil
}
