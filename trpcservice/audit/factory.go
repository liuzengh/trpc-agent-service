package audit

import (
	"database/sql"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type sqlProvider interface{ SQLDB() *sql.DB }

type Options struct {
	SpoolDirectory     string
	MaxBufferedRecords int
}

func NewForControlPlane(repository controlplane.Repository, options ...Options) (Writer, error) {
	if repository == nil {
		return nil, fmt.Errorf("audit control-plane repository is required")
	}
	var base Writer = NewMemoryWriter()
	if provider, ok := repository.(sqlProvider); ok {
		var err error
		base, err = NewPostgresWriter(provider.SQLDB())
		if err != nil {
			return nil, err
		}
	}
	opts := Options{MaxBufferedRecords: 10000}
	if len(options) > 0 {
		opts = options[0]
	}
	return NewPolicyWriter(base, repository, opts.SpoolDirectory, opts.MaxBufferedRecords)
}
