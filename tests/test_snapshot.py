"""Tests for schema snapshots and offline validation."""

import json
import pytest
import tempfile
from pathlib import Path

from sqlp.snapshot import (
    ColumnSnapshot,
    TableSnapshot,
    SchemaSnapshot,
    SchemaRegistry,
)
from sqlp.table import Table
from sqlp.types import Column
from sqlp.pool import AsyncPool


class TestColumnSnapshot:
    """Test ColumnSnapshot serialization."""

    def test_column_snapshot_creation(self) -> None:
        col = ColumnSnapshot(
            name="id",
            python_type="builtins.int",
            primary_key=True,
        )
        assert col.name == "id"
        assert col.python_type == "builtins.int"
        assert col.primary_key is True


class TestTableSnapshot:
    """Test TableSnapshot serialization."""

    def test_table_snapshot_creation(self) -> None:
        col = ColumnSnapshot("id", "builtins.int", primary_key=True)
        table = TableSnapshot(
            name="users",
            columns={"id": col},
            primary_key="id",
        )
        assert table.name == "users"
        assert "id" in table.columns
        assert table.primary_key == "id"


class TestSchemaSnapshot:
    """Test SchemaSnapshot creation and serialization."""

    def test_snapshot_from_tables(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        assert "user" in snapshot.tables
        assert "id" in snapshot.tables["user"].columns
        assert "email" in snapshot.tables["user"].columns

    def test_snapshot_from_multiple_tables(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()
            title = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User, Post)
        assert "user" in snapshot.tables
        assert "post" in snapshot.tables

    def test_snapshot_to_json(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        json_str = snapshot.to_json()

        data = json.loads(json_str)
        assert data["version"] == "1.0"
        assert "user" in data["tables"]
        assert "id" in data["tables"]["user"]["columns"]

    def test_snapshot_roundtrip(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str](unique=True)
            bio = Column[str](nullable=True)

        snapshot1 = SchemaSnapshot.from_tables(User)
        json_str = snapshot1.to_json()
        snapshot2 = SchemaSnapshot.from_json(json_str)

        assert snapshot1.tables.keys() == snapshot2.tables.keys()
        user1 = snapshot1.tables["user"]
        user2 = snapshot2.tables["user"]
        assert user1.columns.keys() == user2.columns.keys()
        assert user1.columns["email"].unique == user2.columns["email"].unique
        assert user1.columns["bio"].nullable == user2.columns["bio"].nullable

    def test_snapshot_to_file(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "schema.json"
            snapshot.to_file(path)
            assert path.exists()

    def test_snapshot_from_file(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot1 = SchemaSnapshot.from_tables(User)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "schema.json"
            snapshot1.to_file(path)
            snapshot2 = SchemaSnapshot.from_file(path)

            assert snapshot1.tables.keys() == snapshot2.tables.keys()


class TestSchemaRegistry:
    """Test SchemaRegistry for offline validation."""

    def test_registry_get_table(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        user_table = registry.get_table("user")
        assert user_table.name == "user"

    def test_registry_table_exists(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        assert registry.table_exists("user")
        assert not registry.table_exists("posts")

    def test_registry_column_exists(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        assert registry.column_exists("user", "id")
        assert registry.column_exists("user", "email")
        assert not registry.column_exists("user", "name")
        assert not registry.column_exists("posts", "id")

    def test_registry_get_column(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str](unique=True)

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        col = registry.get_column("user", "email")
        assert col.name == "email"
        assert col.unique is True

    def test_registry_get_primary_key(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pk = registry.get_primary_key("user")
        assert pk == "id"

    def test_registry_from_snapshot_file(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        snapshot = SchemaSnapshot.from_tables(User)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "schema.json"
            snapshot.to_file(path)
            registry = SchemaRegistry.from_snapshot_file(path)

            assert registry.table_exists("user")

    def test_registry_get_table_not_found(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        with pytest.raises(KeyError, match="not found"):
            registry.get_table("posts")

    def test_registry_get_column_not_found(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        with pytest.raises(KeyError, match="not found"):
            registry.get_column("user", "name")


class TestOfflineValidation:
    """Test offline validation in query builders."""

    def test_select_builder_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.select(User)
        stmt = builder.build()
        assert "SELECT" in stmt.text

    def test_select_builder_invalid_table_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        class Post(Table):
            id = Column[int](primary_key=True)
            title = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)  # Only User
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        with pytest.raises(ValueError, match="not found"):
            pool.select(Post)  # Post not in registry

    def test_select_builder_invalid_column_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.select(User)

        # Add WHERE with non-existent column
        from sqlp.types import ColumnRef

        fake_ref = ColumnRef("name", str)

        with pytest.raises(ValueError, match="not found"):
            builder.where(fake_ref == "test").build()

    def test_insert_builder_invalid_column_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.insert(User)

        with pytest.raises(ValueError, match="not found"):
            builder.values({"id": 1, "name": "Alice"}).build()

    def test_update_builder_invalid_column_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.update(User)

        from sqlp.types import ColumnRef

        user_id = ColumnRef("id", int)

        with pytest.raises(ValueError, match="not found"):
            builder.set(name="Alice").where(user_id == 1).build()

    def test_delete_builder_invalid_column_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.delete(User)

        from sqlp.types import ColumnRef

        fake_ref = ColumnRef("name", str)

        with pytest.raises(ValueError, match="not found"):
            builder.where(fake_ref == "test").build()

    def test_order_by_invalid_column_with_registry(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        snapshot = SchemaSnapshot.from_tables(User)
        registry = SchemaRegistry(snapshot)

        pool = AsyncPool("sqlite://:memory:", registry=registry)
        builder = pool.select(User)

        with pytest.raises(ValueError, match="not found"):
            builder.order_by("name").build()
