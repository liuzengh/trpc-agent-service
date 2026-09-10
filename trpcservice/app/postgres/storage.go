package postgres

import storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"

type queryer = storagepostgres.Queryer

var (
	// ErrStorage reports unavailable or invalid application persistence state.
	ErrStorage   = storagepostgres.ErrStorage
	begin        = storagepostgres.Begin
	rollback     = storagepostgres.Rollback
	commit       = storagepostgres.Commit
	mapDBError   = storagepostgres.MapError
	asUTC        = storagepostgres.AsUTC
	nullableInt  = storagepostgres.NullableInt
	monotonicNow = storagepostgres.MonotonicNow
	encodeJSON   = storagepostgres.EncodeJSON
	decodeJSON   = storagepostgres.DecodeJSON
)

type rowScanner = storagepostgres.RowScanner
