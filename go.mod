module github.com/liuzengh/trpc-agent-service

go 1.24.1

require (
	github.com/alicebob/miniredis/v2 v2.35.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/coreos/go-oidc/v3 v3.17.0
	github.com/go-jose/go-jose/v4 v4.1.3
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.2
	github.com/larksuite/channel-sdk-go v0.1.0
	github.com/larksuite/oapi-sdk-go/v3 v3.10.0
	github.com/mattn/go-sqlite3 v1.14.32
	github.com/prometheus/client_golang v1.20.1
	github.com/redis/go-redis/v9 v9.11.0
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/pkg/kadm v1.15.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.29.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.38.0
	go.opentelemetry.io/otel/exporters/prometheus v0.51.0
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/sdk/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
	go.opentelemetry.io/proto/otlp v1.7.1
	golang.org/x/crypto v0.48.0
	golang.org/x/net v0.49.0
	golang.org/x/oauth2 v0.35.0
	google.golang.org/protobuf v1.36.10
	trpc.group/trpc-go/trpc-agent-go v1.11.2
	trpc.group/trpc-go/trpc-agent-go/artifact/s3 v1.11.0
	trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/golang v1.11.0
	trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/python v1.11.2
	trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/elasticsearch v1.11.0
	trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/pgvector v1.11.0
	trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant v1.11.0
	trpc.group/trpc-go/trpc-agent-go/memory/mysql v1.11.0
	trpc.group/trpc-go/trpc-agent-go/memory/postgres v1.11.0
	trpc.group/trpc-go/trpc-agent-go/memory/redis v1.11.0
	trpc.group/trpc-go/trpc-agent-go/openclaw v0.0.1
	trpc.group/trpc-go/trpc-agent-go/session/clickhouse v1.11.0
	trpc.group/trpc-go/trpc-agent-go/session/mongodb v1.11.0
	trpc.group/trpc-go/trpc-agent-go/session/mysql v1.11.2
	trpc.group/trpc-go/trpc-agent-go/session/postgres v1.11.0
	trpc.group/trpc-go/trpc-agent-go/session/redis v1.11.0
	trpc.group/trpc-go/trpc-agent-go/session/sqlite v1.11.0
	trpc.group/trpc-go/trpc-agent-go/storage/postgres v1.11.0
	trpc.group/trpc-go/trpc-mcp-go v0.0.10
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/ClickHouse/ch-go v0.65.1 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.34.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/aws/aws-sdk-go-v2 v1.32.5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.28.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.46 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.67.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.1 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmatcuk/doublestar/v4 v4.9.1 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/creack/pty v1.1.24 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/elastic/elastic-transport-go/v8 v8.7.0 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.10 // indirect
	github.com/elastic/go-elasticsearch/v8 v8.19.0 // indirect
	github.com/elastic/go-elasticsearch/v9 v9.1.0 // indirect
	github.com/getkin/kin-openapi v0.133.0 // indirect
	github.com/go-ego/gse v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gonfva/docxlib v0.0.0-20210517191039-d8f39cecf1ad // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037 // indirect
	github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90 // indirect
	github.com/openai/openai-go v1.12.0 // indirect
	github.com/panjf2000/ants/v2 v2.10.0 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/pgvector/pgvector-go v0.2.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/qdrant/go-client v1.16.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/tencentyun/cos-go-sdk-v5 v0.7.69 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	github.com/vcaesar/cedar v0.20.2 // indirect
	github.com/woodsbury/decimal128 v1.3.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	github.com/yuin/goldmark v1.7.13 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.mongodb.org/mongo-driver v1.17.7 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.29.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.29.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	trpc.group/trpc-go/trpc-a2a-go v0.2.6-0.20260721084546-18c8244d0acb // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/clickhouse v1.11.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/elasticsearch v0.2.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/mongodb v1.11.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/mysql v1.11.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/qdrant v1.11.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/redis v1.11.0 // indirect
	trpc.group/trpc-go/trpc-agent-go/storage/s3 v1.11.0 // indirect
)
