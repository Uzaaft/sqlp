"""Tests for schema validation."""

import pytest

from sqlp.pool import AsyncPool
from sqlp.schema import validate_schema, SchemaValidator
from sqlp.table import Table
from sqlp.types import Column


class TestSchemaValidation:
    """Test schema validation against database."""

    @pytest.mark.asyncio
    async def test_valid_schema_passes(self) -> None:
        """Test validation passes for matching schema."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Create matching schema
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should not raise
            await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_missing_column_fails(self) -> None:
        """Test validation fails when column is missing from database."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                bio = Column[str]()

            # Create schema without 'bio' column
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should raise
            with pytest.raises(AssertionError, match="'bio'"):
                await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_unexpected_column_fails(self) -> None:
        """Test validation fails for unexpected columns in database."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Create schema with extra column
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                extra_field TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should raise
            with pytest.raises(AssertionError, match="unexpected"):
                await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_wrong_type_fails(self) -> None:
        """Test validation fails for type mismatch."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                age = Column[int]()

            # Create schema with wrong type for 'age'
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                age TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should raise
            with pytest.raises(AssertionError, match="type"):
                await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_nullable_mismatch_fails(self) -> None:
        """Test validation fails when nullability doesn't match."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str](nullable=False)

            # Create schema with nullable column
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should raise
            with pytest.raises(AssertionError, match="NULL"):
                await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_primary_key_mismatch_fails(self) -> None:
        """Test validation fails when primary key doesn't match."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int]()  # Not a primary key
                email = Column[str](primary_key=True)

            # Create schema with different primary key
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should raise
            with pytest.raises(AssertionError, match="primary key"):
                await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_nullable_column_matches(self) -> None:
        """Test validation passes for nullable columns."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                bio = Column[str](nullable=True)

            # Create matching schema
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                bio TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Should not raise
            await validate_schema(pool, User)

    @pytest.mark.asyncio
    async def test_multiple_tables_validation(self) -> None:
        """Test validation of multiple tables."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            class Post(Table):
                id = Column[int](primary_key=True)
                user_id = Column[int]()
                title = Column[str]()

            # Create both schemas
            user_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            post_sql = """
            CREATE TABLE post (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(user_sql, [])
                await conn.execute(post_sql, [])

            # Should not raise
            await validate_schema(pool, User, Post)

    @pytest.mark.asyncio
    async def test_type_compatibility(self) -> None:
        """Test that compatible types pass validation."""
        validator = SchemaValidator(AsyncPool("sqlite://:memory:"))
        
        # These should be compatible
        assert validator._types_compatible("INTEGER", "INT")
        assert validator._types_compatible("INT", "BIGINT")
        assert validator._types_compatible("TEXT", "VARCHAR")
        assert validator._types_compatible("FLOAT", "REAL")
        
        # These should not be compatible
        assert not validator._types_compatible("INTEGER", "TEXT")
        assert not validator._types_compatible("FLOAT", "VARCHAR")

    @pytest.mark.asyncio
    async def test_table_not_found_fails(self) -> None:
        """Test validation fails when table doesn't exist in database."""
        async with AsyncPool("sqlite://:memory:") as pool:
            class User(Table):
                id = Column[int](primary_key=True)

            # Don't create any tables
            
            # Should raise - table not found
            with pytest.raises(AssertionError):
                await validate_schema(pool, User)
