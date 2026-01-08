# pg-tikv Admin CLI

Multi-tenant administration tool for pg-tikv cloud platform.

## Installation

The CLI uses [uv](https://github.com/astral-sh/uv) for dependency management. Dependencies are declared inline using PEP 723.

```bash
cd pg-tikv

# Run directly (uv auto-installs dependencies)
./scripts/pg_tikv_admin.py --help

# Or via uv explicitly
uv run scripts/pg_tikv_admin.py --help
```

Requirements:
- Python 3.8+
- [uv](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- `psql` client (for user management commands)

Dependencies (auto-managed by uv):
- `requests>=2.28.0`

## Configuration

Set environment variables to configure the CLI:

| Variable | Default | Description |
|----------|---------|-------------|
| `PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD addresses (comma-separated) |
| `PG_HOST` | `127.0.0.1` | pg-tikv server host |
| `PG_PORT` | `5433` | pg-tikv server port |

Example:
```bash
export PD_ENDPOINTS="pd1.example.com:2379,pd2.example.com:2379"
export PG_HOST="db.example.com"
export PG_PORT="5433"
```

## Commands

### create-tenant

Create a new tenant with isolated keyspace.

```bash
pg-tikv-admin create-tenant <tenant_name> [options]
```

Options:
- `--admin-user USER` - Admin username (default: `admin`)
- `--password PWD` - Admin password (auto-generated if not specified)

Examples:
```bash
# Create tenant with auto-generated password
pg-tikv-admin create-tenant acme_corp

# Create tenant with specific password
pg-tikv-admin create-tenant acme_corp --password "SecurePass123!"

# Create tenant with custom admin user
pg-tikv-admin create-tenant acme_corp --admin-user dbadmin --password "SecurePass123!"
```

Output:
```
============================================================
TENANT CREATED SUCCESSFULLY
============================================================
Tenant Name:    acme_corp
Admin User:     admin
Password:       BuICsVw7$RHtamoZ

Connection:
  psql -h 127.0.0.1 -p 5433 -U acme_corp.admin

Connection String:
  postgresql://acme_corp.admin:BuICsVw7$RHtamoZ@127.0.0.1:5433/postgres
============================================================
```

### list-tenants

List all tenants.

```bash
pg-tikv-admin list-tenants
```

Output:
```
NAME                           STATE          
---------------------------------------------
acme_corp                      ENABLED        
beta_inc                       ENABLED        
```

JSON output:
```bash
pg-tikv-admin --json list-tenants
```
```json
[
  {"name": "acme_corp", "state": "ENABLED"},
  {"name": "beta_inc", "state": "ENABLED"}
]
```

### get-tenant

Get tenant details.

```bash
pg-tikv-admin get-tenant <tenant_name>
```

Example:
```bash
pg-tikv-admin get-tenant acme_corp
```

Output:
```
Tenant: acme_corp
Host: 127.0.0.1
Port: 5433
User format: acme_corp.<username>
```

### create-user

Create a new user within a tenant.

```bash
pg-tikv-admin create-user <tenant_name> <username> [options]
```

Options:
- `--admin-user USER` - Admin username (default: `admin`)
- `--admin-password PWD` - Admin password (prompted if not specified)
- `--password PWD` - New user's password (auto-generated if not specified)
- `--superuser` - Create user as superuser

Examples:
```bash
# Create regular user
pg-tikv-admin create-user acme_corp developer --admin-password admin

# Create user with specific password
pg-tikv-admin create-user acme_corp developer --admin-password admin --password "DevPass123!"

# Create superuser
pg-tikv-admin create-user acme_corp dba --admin-password admin --superuser
```

Output:
```
User 'developer' created in tenant 'acme_corp'
Password: hy$^G5$EN%*2&x9C
Connection: psql -h 127.0.0.1 -p 5433 -U acme_corp.developer
```

### list-users

List users in a tenant.

```bash
pg-tikv-admin list-users <tenant_name> [options]
```

Options:
- `--admin-user USER` - Admin username (default: `admin`)
- `--admin-password PWD` - Admin password (prompted if not specified)

Example:
```bash
pg-tikv-admin list-users acme_corp --admin-password admin
```

Output:
```
NAME                 SUPERUSER  LOGIN    CREATEDB   CREATEROLE  
------------------------------------------------------------
admin                Yes        Yes      Yes        Yes         
developer            No         Yes      No         No          
```

### reset-password

Reset a user's password.

```bash
pg-tikv-admin reset-password <tenant_name> [options]
```

Options:
- `--admin-user USER` - Admin username (default: `admin`)
- `--admin-password PWD` - Admin password (prompted if not specified)
- `--user USER` - User whose password to reset (default: `admin`)
- `--password PWD` - New password (auto-generated if not specified)

Examples:
```bash
# Reset admin password
pg-tikv-admin reset-password acme_corp --admin-password admin

# Reset specific user's password
pg-tikv-admin reset-password acme_corp --user developer --admin-password admin

# Reset with specific new password
pg-tikv-admin reset-password acme_corp --user developer --admin-password admin --password "NewPass456!"
```

Output:
```
Password reset for user 'developer' in tenant 'acme_corp'
New password: 4FkWJgvJXMi^A3Os
```

### delete-user

Delete a user from a tenant.

```bash
pg-tikv-admin delete-user <tenant_name> <username> [options]
```

Options:
- `--admin-user USER` - Admin username (default: `admin`)
- `--admin-password PWD` - Admin password (prompted if not specified)

Example:
```bash
pg-tikv-admin delete-user acme_corp developer --admin-password admin
```

Output:
```
User 'developer' deleted from tenant 'acme_corp'
```

### delete-tenant

Disable a tenant.

```bash
pg-tikv-admin delete-tenant <tenant_name> [options]
```

Options:
- `--force` - Skip confirmation prompt

Example:
```bash
# Interactive confirmation
pg-tikv-admin delete-tenant acme_corp

# Skip confirmation
pg-tikv-admin delete-tenant acme_corp --force
```

> **Note**: TiKV keyspaces cannot be fully deleted, only disabled. The tenant data remains but becomes inaccessible.

## JSON Output

All commands support `--json` flag for machine-readable output:

```bash
pg-tikv-admin --json list-tenants
pg-tikv-admin --json get-tenant acme_corp
pg-tikv-admin --json create-tenant new_corp
```

## Cloud Platform Integration

### REST API Wrapper

Example Flask wrapper for the CLI:

```python
from flask import Flask, jsonify, request
import subprocess
import json

app = Flask(__name__)

def run_admin(*args):
    result = subprocess.run(
        ["python3", "scripts/pg_tikv_admin.py", "--json"] + list(args),
        capture_output=True, text=True
    )
    if result.returncode == 0:
        return json.loads(result.stdout), 200
    return {"error": result.stderr}, 400

@app.route("/tenants", methods=["GET"])
def list_tenants():
    return jsonify(run_admin("list-tenants")[0])

@app.route("/tenants", methods=["POST"])
def create_tenant():
    data = request.json
    args = ["create-tenant", data["name"]]
    if "password" in data:
        args += ["--password", data["password"]]
    return jsonify(run_admin(*args)[0])

@app.route("/tenants/<name>", methods=["GET"])
def get_tenant(name):
    return jsonify(run_admin("get-tenant", name)[0])
```

### Kubernetes Operator

The CLI can be used in a Kubernetes operator to manage tenants:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: create-tenant-acme
spec:
  template:
    spec:
      containers:
      - name: admin
        image: pg-tikv:latest
        command: ["python3", "scripts/pg_tikv_admin.py"]
        args: ["create-tenant", "acme_corp", "--password", "$(ADMIN_PASSWORD)"]
        env:
        - name: PD_ENDPOINTS
          value: "pd-0.pd:2379,pd-1.pd:2379,pd-2.pd:2379"
        - name: PG_HOST
          value: "pg-tikv.default.svc"
        - name: ADMIN_PASSWORD
          valueFrom:
            secretKeyRef:
              name: tenant-secrets
              key: acme-admin-password
      restartPolicy: Never
```

## Tenant Naming Rules

- Length: 3-64 characters
- Allowed characters: lowercase letters, digits, underscore
- Examples: `acme_corp`, `tenant123`, `my_company`
- Invalid: `Acme-Corp` (uppercase, hyphen), `ab` (too short)

## Connection Format

Users connect using the format `<tenant>.<user>`:

```bash
# Connect as admin of acme_corp tenant
psql -h db.example.com -p 5433 -U acme_corp.admin

# Connect as developer of acme_corp tenant  
psql -h db.example.com -p 5433 -U acme_corp.developer

# Connection string format
postgresql://acme_corp.admin:password@db.example.com:5433/postgres
```

## Security Recommendations

1. **Never use default passwords in production** - Always specify or reset passwords
2. **Store generated passwords securely** - Use a secrets manager
3. **Use TLS** - Configure `PG_TLS_CERT` and `PG_TLS_KEY` on pg-tikv server
4. **Limit admin access** - Create non-superuser accounts for applications
5. **Audit tenant creation** - Log all CLI operations

## Troubleshooting

### Cannot connect to PD

```
Error connecting to PD: Connection refused
```

Check that PD is running and `PD_ENDPOINTS` is correct:
```bash
curl http://127.0.0.1:2379/pd/api/v1/version
```

### Cannot connect to pg-tikv

```
psql: error: connection to server failed
```

Check that pg-tikv is running and `PG_HOST`/`PG_PORT` are correct:
```bash
psql -h 127.0.0.1 -p 5433 -U admin -c "SELECT 1"
```

### Authentication failed

```
FATAL: password authentication failed
```

The default admin password is `admin`. Reset it using:
```bash
pg-tikv-admin reset-password <tenant> --admin-password admin --user admin
```

### Keyspace creation failed

```
Error creating keyspace: 500 Internal Server Error
```

Check PD logs for details. Common causes:
- PD cluster not healthy
- Keyspace name conflicts with reserved names
