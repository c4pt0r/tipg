# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Clients                             │
│                (psql, pgcli, pg_dump, applications)                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ PostgreSQL Wire Protocol
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         pg-tikv Server                              │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    Protocol Layer (pgwire)                    │  │
│  │  • Simple Query Handler    • Extended Query Handler           │  │
│  │  • Startup/Auth Handler    • COPY Handler                     │  │
│  ├───────────────────────────────────────────────────────────────┤  │
│  │                       SQL Layer                               │  │
│  │  • Parser (sqlparser-rs)   • Executor                         │  │
│  │  • Expression Evaluator    • Aggregate Functions              │  │
│  │  • Session Management      • Auth Manager                     │  │
│  ├───────────────────────────────────────────────────────────────┤  │
│  │                     Storage Layer                             │  │
│  │  • Key Encoding           • Schema Management                 │  │
│  │  • Index Management       • Transaction Wrapper               │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ gRPC (TiKV Client Protocol)
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                            TiKV Cluster                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │   TiKV-1    │  │   TiKV-2    │  │   TiKV-3    │                 │
│  │  (Region 1) │  │  (Region 2) │  │  (Region 3) │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
│                          │                                          │
│                    Raft Consensus                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │    PD-1     │  │    PD-2     │  │    PD-3     │                 │
│  │ (Placement) │  │ (Placement) │  │ (Placement) │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Protocol Layer

**Location**: `src/protocol/handler.rs`

Implements the PostgreSQL wire protocol using the `pgwire` crate:

- **StartupHandler**: Connection establishment and authentication
- **SimpleQueryHandler**: Handles `SELECT`, `INSERT`, etc. as text
- **ExtendedQueryHandler**: Handles prepared statements and portals
- **CopyHandler**: Handles `COPY FROM stdin` for bulk loading

Key structures:

```rust
pub struct DynamicPgHandler {
    pd_endpoints: Vec<String>,
    namespace: Option<String>,
    default_keyspace: Option<String>,
    fallback_password: Option<String>,
    executor: OnceCell<Arc<Executor>>,
    session: Mutex<Option<Session>>,
    // ...
}
```

### SQL Layer

#### Parser

**Location**: `src/sql/parser.rs`

Uses `sqlparser-rs` to parse SQL into AST:

```rust
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}
```

#### Executor

**Location**: `src/sql/executor.rs`

Executes parsed SQL statements against TiKV:

```rust
pub struct Executor {
    store: Arc<TikvStore>,
    auth_manager: AuthManager,
}

impl Executor {
    pub async fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecuteResult>;
}
```

Supports:
- DDL: CREATE/DROP/ALTER TABLE, INDEX, VIEW
- DML: INSERT, UPDATE, DELETE
- Queries: SELECT with joins, subqueries, CTEs, window functions
- Auth: CREATE/ALTER/DROP ROLE, GRANT, REVOKE

#### Expression Evaluator

**Location**: `src/sql/expr.rs`

Evaluates SQL expressions:

```rust
pub fn eval_expr(expr: &Expr, row: &Row, schema: &TableSchema) -> Result<Value>;
```

Supports:
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Functions: String, math, date/time
- JSON operators: `->`, `->>`, `@>`, `<@`

#### Session Management

**Location**: `src/sql/session.rs`

Manages transaction state and user context:

```rust
pub struct Session {
    store: Arc<TikvStore>,
    state: TransactionState,
    current_user: Option<String>,
    is_superuser: bool,
}
```

### Storage Layer

#### TiKV Store

**Location**: `src/storage/tikv_store.rs`

Wrapper around TiKV client:

```rust
pub struct TikvStore {
    client: Arc<TransactionClient>,
    namespace: String,
}

impl TikvStore {
    pub async fn new_with_keyspace(
        pd_endpoints: Vec<String>,
        namespace: Option<String>,
        keyspace: Option<String>,
    ) -> Result<Self>;

    pub async fn begin(&self) -> Result<Transaction>;
    pub async fn get_schema(&self, txn: &mut Transaction, table_name: &str) -> Result<Option<TableSchema>>;
    pub async fn scan(&self, txn: &mut Transaction, table_name: &str) -> Result<Vec<Row>>;
    // ...
}
```

#### Key Encoding

**Location**: `src/storage/encoding.rs`

Encodes data for storage in TiKV:

| Key Type | Format |
|----------|--------|
| Schema | `_schema_{table_name}` |
| Data | `_data_{table_id}_{pk_values}` |
| Index | `_idx_{table_id}_{index_id}_{values}` |
| User | `_sys_user_{username}` |
| Role | `_sys_role_{rolename}` |

### Auth Layer

**Location**: `src/auth/`

#### Password Hashing

```rust
// src/auth/password.rs
pub fn hash_password(password: &str, salt: &str) -> String;
pub fn verify_password(password: &str, salt: &str, hash: &str) -> bool;
```

Uses SHA256 with random salt.

#### RBAC

```rust
// src/auth/rbac.rs
pub struct User {
    pub name: String,
    pub password_hash: String,
    pub password_salt: String,
    pub roles: HashSet<String>,
    pub privileges: Vec<GrantedPrivilege>,
    pub is_superuser: bool,
    pub can_login: bool,
    // ...
}

pub struct AuthManager {
    namespace: String,
}

impl AuthManager {
    pub async fn authenticate(&self, txn: &mut Transaction, username: &str, password: &str) -> Result<Option<User>>;
    pub async fn check_privilege(&self, txn: &mut Transaction, username: &str, privilege: &Privilege, object: &PrivilegeObject) -> Result<bool>;
}
```

## Data Flow

### Query Execution

```
1. Client sends query via PostgreSQL protocol
   │
   ▼
2. pgwire parses protocol message
   │
   ▼
3. SimpleQueryHandler receives SQL text
   │
   ▼
4. Parser converts SQL to AST
   │
   ▼
5. Executor processes AST:
   ├── DDL → Modify schema in TiKV
   ├── DML → Modify data in TiKV
   └── Query → Scan/filter/join data
   │
   ▼
6. Results encoded to PostgreSQL protocol
   │
   ▼
7. Client receives response
```

### Authentication Flow

```
1. Client connects
   │
   ▼
2. StartupHandler receives startup message
   │
   ▼
3. Parse username for keyspace (tenant.user)
   │
   ▼
4. Request password (CleartextPassword)
   │
   ▼
5. Authenticate against TiKV-stored users
   ├── Success → Create session with user context
   └── Failure → Close connection
   │
   ▼
6. Initialize executor with keyspace
   │
   ▼
7. Connection ready for queries
```

## Transaction Model

pg-tikv uses TiKV's pessimistic transactions:

```rust
// Automatic transaction for single statements
session.begin().await?;
executor.execute_statement(txn, stmt).await?;
session.commit().await?;

// Explicit transaction block
// BEGIN
session.begin().await?;
// ... multiple statements ...
// COMMIT or ROLLBACK
session.commit().await?;
// or session.rollback().await?;
```

### Isolation Level

TiKV provides Snapshot Isolation by default, which prevents:
- Dirty reads
- Non-repeatable reads
- Phantom reads (for writes)

## Keyspace Isolation

Each keyspace stores data with a keyspace-specific prefix managed by TiKV:

```
Keyspace: tenant_a
  └── _schema_users → {schema data}
  └── _data_1_... → {row data}
  └── _sys_user_admin → {user data}

Keyspace: tenant_b
  └── _schema_users → {different schema}
  └── _data_1_... → {different rows}
  └── _sys_user_admin → {different admin}
```

Keyspaces are completely isolated at the TiKV level.

## Performance Considerations

### Connection Pooling

Each connection creates:
- One TiKV client
- One executor
- One session

For high-concurrency, use a connection pooler like PgBouncer.

### Index Usage

Indexes are automatically used when filtering by indexed columns:

```sql
-- Uses index on email
SELECT * FROM users WHERE email = 'alice@example.com';

-- Full table scan (no index on name)
SELECT * FROM users WHERE name = 'Alice';
```

### Transaction Size

Large transactions consume memory. Break into smaller batches:

```sql
-- Instead of one huge transaction
-- Use multiple smaller ones
BEGIN; INSERT INTO ... (1000 rows); COMMIT;
BEGIN; INSERT INTO ... (1000 rows); COMMIT;
```

## Limitations

### Not Supported

- RIGHT JOIN, FULL OUTER JOIN
- Recursive CTEs (`WITH RECURSIVE`)
- Materialized views
- Foreign keys (parsed but not enforced)
- CHECK constraints (parsed but not enforced)
- Stored procedures / functions
- Triggers
- Multiple databases per keyspace
- LISTEN/NOTIFY

### Differences from PostgreSQL

1. **No pg_catalog**: System catalogs not implemented
2. **Limited type system**: Fewer data types than PostgreSQL
3. **No planner**: Simple execution without cost-based optimization
4. **Single-node SQL**: Queries execute on pg-tikv, not pushed to TiKV

## Future Improvements

- [ ] Query planner with cost-based optimization
- [ ] Push-down predicates to TiKV
- [ ] Parallel query execution
- [ ] Connection pooling
- [ ] SSL/TLS support
- [ ] More PostgreSQL compatibility
