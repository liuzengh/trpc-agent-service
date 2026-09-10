package mysql

import storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"

var (
	// ErrStorage reports unavailable or invalid tenant persistence state.
	ErrStorage     = storagemysql.ErrStorage
	begin          = storagemysql.Begin
	beginConn      = storagemysql.BeginConn
	acquireLock    = storagemysql.AcquireLock
	releaseLock    = storagemysql.ReleaseLock
	rollback       = storagemysql.Rollback
	commit         = storagemysql.Commit
	mapDBError     = storagemysql.MapError
	asUTC          = storagemysql.AsUTC
	nullableInt    = storagemysql.NullableInt
	nullableString = storagemysql.NullableString
	nullableText   = storagemysql.NullableText
)

type rowScanner = storagemysql.RowScanner
