package postgres

import storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"

var (
	// ErrStorage reports unavailable or invalid tenant persistence state.
	ErrStorage     = storagepostgres.ErrStorage
	begin          = storagepostgres.Begin
	rollback       = storagepostgres.Rollback
	commit         = storagepostgres.Commit
	mapDBError     = storagepostgres.MapError
	asUTC          = storagepostgres.AsUTC
	nullableInt    = storagepostgres.NullableInt
	nullableString = storagepostgres.NullableString
	nullableText   = storagepostgres.NullableText
)

type rowScanner = storagepostgres.RowScanner
