"""Schema validation and verification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlp.pool import AsyncPool
from sqlp.table import Table
from sqlp.types import Column, PostgreSQLAdapter, SQLiteAdapter, MySQLAdapter


@dataclass
class ColumnInfo:
    """Information about a database column."""

    name: str
    db_type: str
    nullable: bool
    is_primary_key: bool = False


class SchemaValidator:
    """Validates that Table definitions match actual database schema."""

    def __init__(self, pool: AsyncPool) -> None:
        assert pool is not None, "Pool cannot be None"
        self.pool = pool
        self._adapter = self._get_adapter()

    def _get_adapter(self) -> Any:
        """Get the appropriate type adapter for the database."""
        if self.pool.db_type == "postgresql":
            return PostgreSQLAdapter()
        elif self.pool.db_type == "sqlite":
            return SQLiteAdapter()
        elif self.pool.db_type == "mysql":
            return MySQLAdapter()
        raise ValueError(f"Unsupported database type: {self.pool.db_type}")

    async def validate_schema(self, *tables: type) -> None:
        """Validate that all tables match the database schema.

        Raises AssertionError if any mismatches are found.
        """
        assert tables, "At least one table required"
        
        for table in tables:
            await self._validate_table(table)

    async def _validate_table(self, table: type) -> None:
        """Validate a single table."""
        table_name = table.__table_name__()
        expected_columns = table.__columns__()
        
        # Get actual schema from database
        actual_columns = await self._get_db_columns(table_name)
        
        # Check all expected columns exist
        for col_name, col in expected_columns.items():
            if col_name not in actual_columns:
                raise AssertionError(
                    f"Column '{col_name}' in table '{table_name}' not found in database"
                )
            
            # Validate the column
            self._validate_column(
                table_name,
                col_name,
                col,
                actual_columns[col_name],
            )
        
        # Check for unexpected columns in database
        unexpected = set(actual_columns.keys()) - set(expected_columns.keys())
        if unexpected:
            raise AssertionError(
                f"Table '{table_name}' has unexpected columns in database: {', '.join(unexpected)}"
            )

    def _validate_column(
        self,
        table_name: str,
        col_name: str,
        expected: Column,
        actual: ColumnInfo,
    ) -> None:
        """Validate a single column definition."""
        # Check type match
        expected_db_type = self._adapter.python_to_db(expected.python_type)
        
        # Normalize DB types for comparison (some DBs use aliases)
        if not self._types_compatible(expected_db_type, actual.db_type):
            raise AssertionError(
                f"Column '{col_name}' in table '{table_name}': "
                f"expected type {expected_db_type}, got {actual.db_type}"
            )
        
        # Check primary key match first
        if expected.primary_key != actual.is_primary_key:
            pk_str = "primary key" if expected.primary_key else "regular column"
            actual_str = "primary key" if actual.is_primary_key else "regular column"
            raise AssertionError(
                f"Column '{col_name}' in table '{table_name}': "
                f"expected {pk_str}, got {actual_str}"
            )
        
        # Check nullable match (but primary keys are implicitly NOT NULL)
        # SQLite incorrectly reports primary keys as nullable, so we skip this check for PKs
        if not expected.primary_key and expected.nullable != actual.nullable:
            actual_nullable_str = "NULL" if actual.nullable else "NOT NULL"
            expected_nullable_str = "NULL" if expected.nullable else "NOT NULL"
            raise AssertionError(
                f"Column '{col_name}' in table '{table_name}': "
                f"expected {expected_nullable_str}, got {actual_nullable_str}"
            )

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if database types are compatible."""
        # Normalize types (handle VARCHAR(255) vs VARCHAR, etc)
        expected_base = expected.split("(")[0].upper()
        actual_base = actual.split("(")[0].upper()
        
        # Direct match
        if expected_base == actual_base:
            return True
        
        # Handle type aliases
        aliases = {
            "INTEGER": ["INT", "BIGINT", "SMALLINT"],
            "INT": ["INTEGER", "BIGINT", "SMALLINT"],
            "TEXT": ["VARCHAR", "CHAR", "LONGTEXT"],
            "VARCHAR": ["TEXT", "CHAR"],
            "FLOAT": ["DOUBLE", "REAL"],
            "NUMERIC": ["DECIMAL"],
            "DECIMAL": ["NUMERIC"],
        }
        
        if expected_base in aliases:
            return actual_base in aliases[expected_base] or actual_base == expected_base
        
        return False

    async def _get_db_columns(self, table_name: str) -> dict[str, ColumnInfo]:
        """Get column information from database."""
        if self.pool.db_type == "postgresql":
            return await self._get_postgresql_columns(table_name)
        elif self.pool.db_type == "sqlite":
            return await self._get_sqlite_columns(table_name)
        elif self.pool.db_type == "mysql":
            return await self._get_mysql_columns(table_name)
        raise ValueError(f"Unsupported database type: {self.pool.db_type}")

    async def _get_postgresql_columns(self, table_name: str) -> dict[str, ColumnInfo]:
        """Get columns from PostgreSQL."""
        query = f"""
        SELECT column_name, data_type, is_nullable, 
               (SELECT EXISTS(
                   SELECT 1 FROM information_schema.table_constraints tc
                   WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_name = t.table_name
                   AND tc.constraint_name LIKE '%' || c.column_name || '%'
               )) as is_pk
        FROM information_schema.columns c
        JOIN information_schema.tables t ON c.table_name = t.table_name
        WHERE c.table_name = %s AND t.table_schema = 'public'
        """
        
        async with self.pool._get_connection() as conn:
            rows = await conn.fetch_all(query, [table_name])
        
        columns: dict[str, ColumnInfo] = {}
        for row in rows:
            columns[row["column_name"]] = ColumnInfo(
                name=row["column_name"],
                db_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                is_primary_key=row["is_pk"],
            )
        
        return columns

    async def _get_sqlite_columns(self, table_name: str) -> dict[str, ColumnInfo]:
        """Get columns from SQLite."""
        query = f"PRAGMA table_info({table_name})"
        
        async with self.pool._get_connection() as conn:
            rows = await conn.fetch_all(query, [])
        
        columns: dict[str, ColumnInfo] = {}
        for row in rows:
            columns[row["name"]] = ColumnInfo(
                name=row["name"],
                db_type=row["type"],
                nullable=row["notnull"] == 0,
                is_primary_key=row["pk"] == 1,
            )
        
        return columns

    async def _get_mysql_columns(self, table_name: str) -> dict[str, ColumnInfo]:
        """Get columns from MySQL."""
        query = f"""
        SELECT column_name, column_type, is_nullable,
               column_key = 'PRI' as is_pk
        FROM information_schema.columns
        WHERE table_name = %s
        """
        
        async with self.pool._get_connection() as conn:
            rows = await conn.fetch_all(query, [table_name])
        
        columns: dict[str, ColumnInfo] = {}
        for row in rows:
            columns[row["COLUMN_NAME"]] = ColumnInfo(
                name=row["COLUMN_NAME"],
                db_type=row["COLUMN_TYPE"],
                nullable=row["IS_NULLABLE"] == "YES",
                is_primary_key=row["is_pk"],
            )
        
        return columns


async def validate_schema(pool: AsyncPool, *tables: type) -> None:
    """Validate database schema against Table definitions.
    
    Args:
        pool: AsyncPool connected to database
        tables: Table classes to validate
    
    Raises:
        AssertionError: If any schema mismatches are found
    """
    validator = SchemaValidator(pool)
    await validator.validate_schema(*tables)
