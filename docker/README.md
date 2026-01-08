# pg-tikv Docker Test Environment

Docker setup for testing pg-tikv with TiKV using API v2.

## Quick Start

```bash
cd docker

# Build and start all services
docker compose -f docker-compose.test.yml up --build

# Run tests only (after services are up)
docker compose -f docker-compose.test.yml run --rm test

# Stop and cleanup
docker compose -f docker-compose.test.yml down -v
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| `pd` | 2379 | TiKV Placement Driver |
| `tikv` | 20160 | TiKV storage (API v2 enabled) |
| `pg-tikv` | 5433 | PostgreSQL-compatible layer |
| `test` | - | Test runner (one-shot) |

## TiKV API v2 Configuration

TiKV is configured with API v2 via `tikv.toml`:

```toml
[storage]
api-version = 2
enable-ttl = true
```

## Connect with psql

```bash
# After services are running
psql -h localhost -p 5433 -U postgres -d postgres
# Password: postgres
```

## Run Custom SQL

```bash
# Interactive
docker compose -f docker-compose.test.yml exec pg-tikv \
  psql -h localhost -p 5433 -U postgres

# From file
docker compose -f docker-compose.test.yml exec pg-tikv \
  psql -h localhost -p 5433 -U postgres -f /path/to/file.sql
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PD_ENDPOINTS` | `pd:2379` | TiKV PD address |
| `PG_PORT` | `5433` | PostgreSQL listen port |
| `PG_PASSWORD` | `postgres` | Default password |
| `RUST_LOG` | `info` | Log level |

## Volumes

| Volume | Description |
|--------|-------------|
| `pd_data` | PD metadata |
| `tikv_data` | TiKV data |

## Troubleshooting

### Check service logs

```bash
docker compose -f docker-compose.test.yml logs pd
docker compose -f docker-compose.test.yml logs tikv
docker compose -f docker-compose.test.yml logs pg-tikv
```

### Verify TiKV API v2

```bash
docker compose -f docker-compose.test.yml exec tikv \
  curl -s http://localhost:20180/config | grep api-version
```

### Reset everything

```bash
docker compose -f docker-compose.test.yml down -v
docker compose -f docker-compose.test.yml up --build
```
