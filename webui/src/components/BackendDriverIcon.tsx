import {
  CloudIcon,
  DataIcon,
  DatabaseIcon,
  PostgreSQLBrandIcon,
  RedisBrandIcon,
  VectorIcon,
} from './Icons'

export function BackendDriverIcon({ driver, size = 18 }: { driver: string; size?: number }) {
  switch (driver.toLowerCase()) {
    case 'postgres':
    case 'postgresql':
    case 'pgvector':
      return <PostgreSQLBrandIcon size={size} />
    case 'redis':
      return <RedisBrandIcon size={size} />
    case 'mysql':
    case 'sqlite':
    case 'mongodb':
    case 'clickhouse':
      return <DatabaseIcon size={size} />
    case 'qdrant':
    case 'elasticsearch':
    case 'chromadb':
      return <VectorIcon size={size} />
    case 's3':
    case 'cos':
    case 'tencentdb':
      return <CloudIcon size={size} />
    case 'inmemory':
    case 'mem0':
      return <DataIcon size={size} />
    default:
      return <DatabaseIcon size={size} />
  }
}
