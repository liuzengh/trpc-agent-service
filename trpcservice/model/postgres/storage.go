package postgres

import storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"

var (
	// ErrStorage reports unavailable or invalid model persistence state.
	ErrStorage   = storagepostgres.ErrStorage
	begin        = storagepostgres.Begin
	rollback     = storagepostgres.Rollback
	commit       = storagepostgres.Commit
	mapDBError   = storagepostgres.MapError
	asUTC        = storagepostgres.AsUTC
	nullableText = storagepostgres.NullableText
	monotonicNow = storagepostgres.MonotonicNow
	encodeJSON   = storagepostgres.EncodeJSON
	decodeJSON   = storagepostgres.DecodeJSON
)

type rowScanner = storagepostgres.RowScanner
