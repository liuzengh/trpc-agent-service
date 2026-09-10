package mysql

import storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"

type queryer = storagemysql.Queryer

var (
	// ErrStorage reports unavailable or invalid backend persistence state.
	ErrStorage   = storagemysql.ErrStorage
	begin        = storagemysql.Begin
	rollback     = storagemysql.Rollback
	commit       = storagemysql.Commit
	mapDBError   = storagemysql.MapError
	asUTC        = storagemysql.AsUTC
	monotonicNow = storagemysql.MonotonicNow
	nullableText = storagemysql.NullableText
	encodeJSON   = storagemysql.EncodeJSON
	decodeJSON   = storagemysql.DecodeJSON
)

type rowScanner = storagemysql.RowScanner
