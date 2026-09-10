package mysql

import storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"

type queryer = storagemysql.Queryer

var (
	// ErrStorage reports unavailable or invalid application persistence state.
	ErrStorage   = storagemysql.ErrStorage
	begin        = storagemysql.Begin
	rollback     = storagemysql.Rollback
	commit       = storagemysql.Commit
	mapDBError   = storagemysql.MapError
	asUTC        = storagemysql.AsUTC
	nullableInt  = storagemysql.NullableInt
	nullableText = storagemysql.NullableText
	monotonicNow = storagemysql.MonotonicNow
	encodeJSON   = storagemysql.EncodeJSON
	decodeJSON   = storagemysql.DecodeJSON
)

type rowScanner = storagemysql.RowScanner
