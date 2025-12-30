"""Schema snapshots for offline type validation (sqlx-style caching)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from sqlp.table import Table


@dataclass
class ColumnSnapshot:
    """Serializable column metadata."""

    name: str
    python_type: str  # Fully qualified type name
    primary_key: bool = False
    unique: bool = False
    nullable: bool = False
    default: Any = None
    default_factory: bool = False  # Only store presence, not the callable


@dataclass
class TableSnapshot:
    """Serializable table metadata."""

    name: str
    columns: dict[str, ColumnSnapshot]
    primary_key: str | None = None


@dataclass
class SchemaSnapshot:
    """Complete schema snapshot for offline validation."""

    tables: dict[str, TableSnapshot]
    version: str = "1.0"

    @staticmethod
    def from_tables(*table_classes: type) -> SchemaSnapshot:
        """Create snapshot from Table class definitions (no DB needed)."""
        assert table_classes, "At least one table required"

        tables: dict[str, TableSnapshot] = {}
        for table_class in table_classes:
            assert issubclass(table_class, Table), (
                f"{table_class.__name__} must be a Table subclass"
            )

            table_name = table_class.__table_name__()
            columns_dict = table_class.__columns__()

            columns: dict[str, ColumnSnapshot] = {}
            for col_name, col in columns_dict.items():
                # Get fully qualified type name
                python_type = col.python_type
                assert python_type is not None, (
                    f"Column {col_name} has no python_type set"
                )

                type_name = (
                    python_type.__module__ + "." + python_type.__qualname__
                    if hasattr(python_type, "__module__")
                    else str(python_type)
                )

                columns[col_name] = ColumnSnapshot(
                    name=col_name,
                    python_type=type_name,
                    primary_key=col.primary_key,
                    unique=col.unique,
                    nullable=col.nullable,
                    default=col.default,
                    default_factory=col.default_factory is not None,
                )

            pk = table_class.__primary_key__()
            tables[table_name] = TableSnapshot(
                name=table_name,
                columns=columns,
                primary_key=pk,
            )

        return SchemaSnapshot(tables=tables)

    def to_json(self) -> str:
        """Serialize snapshot to JSON string."""
        return json.dumps(asdict(self), indent=2)

    def to_file(self, path: str | Path) -> None:
        """Write snapshot to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json())

    @staticmethod
    def from_json(json_str: str) -> SchemaSnapshot:
        """Deserialize snapshot from JSON string."""
        data = json.loads(json_str)
        assert data["version"] == "1.0", (
            f"Unsupported schema version: {data['version']}"
        )

        tables: dict[str, TableSnapshot] = {}
        for table_name, table_data in data["tables"].items():
            columns: dict[str, ColumnSnapshot] = {}
            for col_name, col_data in table_data["columns"].items():
                columns[col_name] = ColumnSnapshot(**col_data)

            tables[table_name] = TableSnapshot(
                name=table_data["name"],
                columns=columns,
                primary_key=table_data.get("primary_key"),
            )

        return SchemaSnapshot(tables=tables)

    @staticmethod
    def from_file(path: str | Path) -> SchemaSnapshot:
        """Load snapshot from JSON file."""
        path = Path(path)
        assert path.exists(), f"Snapshot file not found: {path}"
        return SchemaSnapshot.from_json(path.read_text())


class SchemaRegistry:
    """In-memory schema registry for offline validation."""

    def __init__(self, snapshot: SchemaSnapshot) -> None:
        """Initialize registry with a snapshot."""
        assert snapshot is not None, "Snapshot cannot be None"
        self.snapshot = snapshot

    @classmethod
    def from_snapshot_file(cls, path: str | Path) -> SchemaRegistry:
        """Load registry from snapshot file."""
        snapshot = SchemaSnapshot.from_file(path)
        return cls(snapshot)

    def get_table(self, table_name: str) -> TableSnapshot:
        """Get table snapshot by name."""
        if table_name not in self.snapshot.tables:
            raise KeyError(f"Table '{table_name}' not found in schema")
        return self.snapshot.tables[table_name]

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in schema."""
        return table_name in self.snapshot.tables

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if column exists in table."""
        if not self.table_exists(table_name):
            return False
        table = self.get_table(table_name)
        return column_name in table.columns

    def get_column(self, table_name: str, column_name: str) -> ColumnSnapshot:
        """Get column snapshot by table and column name."""
        table = self.get_table(table_name)
        if column_name not in table.columns:
            raise KeyError(f"Column '{column_name}' not found in table '{table_name}'")
        return table.columns[column_name]

    def get_primary_key(self, table_name: str) -> str | None:
        """Get primary key column name for table."""
        table = self.get_table(table_name)
        return table.primary_key

    def validate_column_type(
        self, table_name: str, column_name: str, expected_type: str
    ) -> bool:
        """Validate column type matches expected type."""
        col = self.get_column(table_name, column_name)
        return col.python_type == expected_type


def get_config_snapshot_path() -> Path | None:
    """Get snapshot path from pyproject.toml config."""
    import tomllib

    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        return None

    try:
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        tool_config = config.get("tool", {}).get("sqlp", {})
        snapshot_path = tool_config.get("schema_snapshot_path")

        if snapshot_path:
            return Path(snapshot_path)
    except Exception:
        pass

    return None


def load_schema_registry() -> SchemaRegistry | None:
    """Load schema registry from config path if available."""
    snapshot_path = get_config_snapshot_path()
    if not snapshot_path or not snapshot_path.exists():
        return None

    try:
        return SchemaRegistry.from_snapshot_file(snapshot_path)
    except Exception:
        return None


def should_validate_with_snapshot() -> bool:
    """Check if validation should use snapshot based on env var."""
    source = os.getenv("SQLP_SCHEMA_SOURCE", "database").lower()
    return source == "snapshot"
