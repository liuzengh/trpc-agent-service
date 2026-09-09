param(
    [int]$Count = 5,
    [string]$Package = './pkg/reliable'
)

$ErrorActionPreference = 'Stop'
go test -buildvcs=false $Package -run '^$' -bench '^BenchmarkMemoryStoreInboxOutbox$' -benchtime="${Count}s" -benchmem

