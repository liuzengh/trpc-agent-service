package mysql

import storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"

var (
	// ErrStorage reports unavailable or invalid channel persistence state.
	ErrStorage   = storagemysql.ErrStorage
	begin        = storagemysql.Begin
	rollback     = storagemysql.Rollback
	commit       = storagemysql.Commit
	mapDBError   = storagemysql.MapError
	asUTC        = storagemysql.AsUTC
	monotonicNow = storagemysql.MonotonicNow
	encodeJSON   = storagemysql.EncodeJSON
	decodeJSON   = storagemysql.DecodeJSON
	nullableText = storagemysql.NullableText
)

type rowScanner = storagemysql.RowScanner
