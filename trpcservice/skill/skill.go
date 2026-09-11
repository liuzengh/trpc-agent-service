// Package skill exposes the platform's Agent Skills repository and wires it to
// tRPC-Agent-Go's skill loading. Skills are folders with a SKILL.md file.
package skill

import (
	"context"
	"errors"
	"os"

	"trpc.group/trpc-go/trpc-agent-go/skill"
)

// VisibilityFilter snapshots a tenant's exact-name grants for the framework's
// context-aware skill listing, loading and document tools. Empty denies all.
func VisibilityFilter(names []string) skill.VisibilityFilter {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	return func(_ context.Context, summary skill.Summary) bool {
		_, ok := allowed[summary.Name]
		return ok
	}
}

// EnvSkillsRoot is the environment variable pointing at the skills repository
// root. It reuses the framework-wide variable so operators only configure it
// once.
const EnvSkillsRoot = skill.EnvSkillsRoot

// Repository loads a runnable skill repository rooted at root. Every
// subdirectory must contain a SKILL.md (optional YAML front matter + Markdown
// body); auxiliary files next to it are exposed as skill documents.
func Repository(root string) (*skill.FSRepository, error) {
	if root == "" {
		return nil, errors.New("skill repository root is empty")
	}
	return skill.NewFSRepository(root)
}

// DefaultRepository returns the repository described by EnvSkillsRoot. It
// returns nil (and no error) when no root is configured, meaning the agent
// runs without skills.
func DefaultRepository() (*skill.FSRepository, error) {
	root := os.Getenv(EnvSkillsRoot)
	if root == "" {
		return nil, nil
	}
	return Repository(root)
}
