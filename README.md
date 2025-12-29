# sqlp

Async-first, Pythonic ORM with Pydantic integration and Drizzle-like API.

## Features

- **Async-first**: Built on asyncio with asyncpg, aiosqlite, and aiomysql drivers
- **Type-safe**: Full Python type hints with compile-time checking
- **Pythonic API**: Operator overloading, context managers, and fluent interfaces
- **Pydantic integration**: Automatic row mapping to Pydantic BaseModel
- **Prepared statements**: Automatic caching with LRU and TTL control
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

## Development

```bash
nix develop  # Enter development shell
uv sync     # Install dependencies
pytest      # Run tests
```
