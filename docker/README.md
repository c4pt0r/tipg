# pg-tikv Docker Test Environment

Docker setup for testing pg-tikv with TiKV using API v2.

## Quick Start

```bash
cd docker

# Run all tests (SQL + ORM)
docker compose -f docker-compose.test.yml run --rm test-all

# Or start services and run tests separately
docker compose -f docker-compose.test.yml up -d pg-tikv
docker compose -f docker-compose.test.yml run --rm sql-test
docker compose -f docker-compose.test.yml run --rm orm-test

# Stop and cleanup
docker compose -f docker-compose.test.yml down -v
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| `pd` | 2379 | TiKV Placement Driver |
| `tikv` | 20160 | TiKV storage (API v2 enabled) |
| `pg-tikv` | 5433 | PostgreSQL-compatible layer |
| `sql-test` | - | SQL test runner |
| `orm-test` | - | ORM test runner (TypeORM, Prisma, etc.) |
| `test-all` | - | Run all tests |

## Test Commands

```bash
# Run all tests
docker compose -f docker-compose.test.yml run --rm test-all

# Run only SQL tests
docker compose -f docker-compose.test.yml run --rm sql-test

# Run only ORM tests
docker compose -f docker-compose.test.yml run --rm orm-test

# Run with rebuild
docker compose -f docker-compose.test.yml up --build -d pg-tikv
docker compose -f docker-compose.test.yml run --rm test-all
```

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
cat my_test.sql | docker compose -f docker-compose.test.yml exec -T pg-tikv \
  psql -h localhost -p 5433 -U postgres
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

## CI/CD Integration

For GitHub Actions or other CI systems:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Run tests
        run: |
          cd docker
          docker compose -f docker-compose.test.yml run --rm test-all
          
      - name: Cleanup
        if: always()
        run: |
          cd docker
          docker compose -f docker-compose.test.yml down -v
```

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
docker compose -f docker-compose.test.yml up --build -d pg-tikv
```

### ORM test failures

If ORM tests fail, check:

1. pg-tikv is healthy:
   ```bash
   docker compose -f docker-compose.test.yml ps
   ```

2. Run ORM tests manually with verbose output:
   ```bash
   docker compose -f docker-compose.test.yml run --rm orm-test npm test -- --reporter=verbose
   ```
