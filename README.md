# sqlp

Async-first, Pythonic ORM with Pydantic integration and Drizzle-like API. Includes sqlx-style offline schema validation for zero-database local development.

## Features

- **Async-first**: Built on asyncio with asyncpg, aiosqlite, and aiomysql drivers
- **Type-safe**: Full Python type hints with compile-time checking
- **Pythonic API**: Operator overloading, context managers, and fluent interfaces
- **Pydantic integration**: Automatic row mapping to Pydantic BaseModel
- **Prepared statements**: Automatic caching with LRU and TTL control
- **Offline schema validation**: Query validation without a database connection (sqlx-style snapshots)
- **No magic**: Explicit is better than implicit; fail-fast on errors

## Installation

```bash
uv add sqlp
```

## Quick Start

```python
import asyncio
from sqlp import Table, Column, AsyncPool
from datetime import datetime

class User(Table):
    id: int = Column(primary_key=True)
    email: str = Column(unique=True)
    created_at: datetime = Column()

async def main():
    async with AsyncPool("postgresql://localhost/mydb") as pool:
        users = await pool.select(User).where(User.email.like("%@example.com")).all()
        for user in users:
            print(f"{user.email}: {user.created_at}")

asyncio.run(main())
```

## Offline Schema Validation

sqlp includes sqlx-style schema snapshots for validating queries without a database connection—perfect for CI, local development, and test environments.

### Generate Schema Snapshot

```python
from sqlp.snapshot import SchemaSnapshot

class User(Table):
    id: int = Column(primary_key=True)
    email: str = Column(unique=True)
    name: str = Column()

class Post(Table):
    id: int = Column(primary_key=True)
    user_id: int = Column()
    title: str = Column()

# Generate snapshot from Table definitions (no DB needed)
snapshot = SchemaSnapshot.from_tables(User, Post)
snapshot.to_file(".sqlp/schema.json")  # Commit to version control
```

### Configure Snapshot Path

Add to `pyproject.toml`:

```toml
[tool.sqlp]
schema_snapshot_path = ".sqlp/schema.json"
```

### Validate Queries Offline

```python
import os
from sqlp import AsyncPool

# Use snapshot for validation (no DB connection required)
os.environ["SQLP_SCHEMA_SOURCE"] = "snapshot"

pool = AsyncPool("postgresql://...")  # Registry auto-loaded from .sqlp/schema.json
stmt = pool.select(User).where(User.email == "test@example.com").build()
# ✓ Query validates against snapshot at build() time
```

### Validation Modes

Set `SQLP_SCHEMA_SOURCE` environment variable:

- `"snapshot"` - Validate against `.sqlp/schema.json` (no DB needed, fast, offline)
- `"database"` - Validate against actual database schema (default if unset)

```python
# CI/local dev: validate offline
export SQLP_SCHEMA_SOURCE=snapshot
pytest  # All queries validate without spinning up databases

# Production: sync with actual database
export SQLP_SCHEMA_SOURCE=database
from sqlp.schema import validate_schema
await validate_schema(pool, User, Post)
```

### What Gets Validated

TODO


## Development

```bash
nix develop  # Enter development shell
uv sync     # Install dependencies
pytest      # Run tests
```
