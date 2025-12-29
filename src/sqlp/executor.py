"""Async query execution with result mapping."""

from __future__ import annotations

from typing import Any, AsyncGenerator, Generic, TypeVar, Union
from pydantic import BaseModel

from sqlp.sql import SelectQueryBuilder, SQLStatement
from sqlp.table import Table
from sqlp.pool import AsyncPool

T = TypeVar("T", bound=BaseModel)


class ExecutableQuery(Generic[T]):
    """Wraps a query builder with execution methods."""

    def __init__(
        self,
        builder: SelectQueryBuilder,
        pool: AsyncPool,
        table: type,
    ) -> None:
        assert builder is not None, "Builder cannot be None"
        assert pool is not None, "Pool cannot be None"
        assert table is not None, "Table cannot be None"
        
        self.builder = builder
        self.pool = pool
        self.table = table

    async def first(self) -> T | None:
        """Fetch first result row and map to table model."""
        stmt = self.builder.limit(1).build()
        row = await self.pool.fetch_one(stmt)
        
        if row is None:
            return None
        
        model = self.table.__row_model__()
        return model(**row)

    async def all(self) -> list[T]:
        """Fetch all result rows and map to table model."""
        stmt = self.builder.build()
        rows = await self.pool.fetch_all(stmt)
        
        model = self.table.__row_model__()
        return [model(**row) for row in rows]

    async def count(self) -> int:
        """Count matching rows."""
        # Build a COUNT(*) query
        stmt = self.builder.build()
        # Replace SELECT * with SELECT COUNT(*)
        count_sql = stmt.text.replace("SELECT *", "SELECT COUNT(*) as count", 1)
        count_stmt = SQLStatement(count_sql, stmt.parameters)
        
        row = await self.pool.fetch_one(count_stmt)
        if row is None:
            return 0
        
        return row.get("count", 0)

    async def __aiter__(self) -> AsyncGenerator[T, None]:
        """Async iteration over results."""
        rows = await self.all()
        for row in rows:
            yield row


class ExecutableMutation:
    """Wraps a mutation builder with execution methods."""

    def __init__(
        self,
        builder: Any,  # InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder
        pool: AsyncPool,
    ) -> None:
        assert builder is not None, "Builder cannot be None"
        assert pool is not None, "Pool cannot be None"
        
        self.builder = builder
        self.pool = pool

    async def execute(self) -> int:
        """Execute mutation and return affected row count."""
        stmt = self.builder.build()
        return await self.pool.execute(stmt)


def attach_execution(
    builder: SelectQueryBuilder,
    pool: AsyncPool,
    table: type,
) -> ExecutableQuery:
    """Attach async execution methods to a query builder."""
    return ExecutableQuery(builder, pool, table)


def attach_mutation_execution(
    builder: Any,
    pool: AsyncPool,
) -> ExecutableMutation:
    """Attach async execution methods to a mutation builder."""
    return ExecutableMutation(builder, pool)
