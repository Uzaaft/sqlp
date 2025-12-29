"""Type system and column metadata for sqlp."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID


class TypeAdapter(ABC):
    """Base class for database-specific type adapters."""

    @abstractmethod
    def python_to_db(self, python_type: type) -> str:
        """Convert Python type to database type string."""
        raise NotImplementedError

    @abstractmethod
    def is_supported(self, python_type: type) -> bool:
        """Check if type is supported by this adapter."""
        raise NotImplementedError


class PostgreSQLAdapter(TypeAdapter):
    """PostgreSQL type adapter."""

    _TYPE_MAP = {
        int: "INTEGER",
        str: "TEXT",
        bool: "BOOLEAN",
        float: "FLOAT8",
        bytes: "BYTEA",
        UUID: "UUID",
        datetime: "TIMESTAMP",
        Decimal: "NUMERIC",
    }

    def python_to_db(self, python_type: type) -> str:
        if python_type not in self._TYPE_MAP:
            raise ValueError(f"Unsupported type for PostgreSQL: {python_type}")
        return self._TYPE_MAP[python_type]

    def is_supported(self, python_type: type) -> bool:
        return python_type in self._TYPE_MAP


class SQLiteAdapter(TypeAdapter):
    """SQLite type adapter."""

    _TYPE_MAP = {
        int: "INTEGER",
        str: "TEXT",
        bool: "INTEGER",
        float: "REAL",
        bytes: "BLOB",
        UUID: "TEXT",
        datetime: "TEXT",
        Decimal: "TEXT",
    }

    def python_to_db(self, python_type: type) -> str:
        if python_type not in self._TYPE_MAP:
            raise ValueError(f"Unsupported type for SQLite: {python_type}")
        return self._TYPE_MAP[python_type]

    def is_supported(self, python_type: type) -> bool:
        return python_type in self._TYPE_MAP


class MySQLAdapter(TypeAdapter):
    """MySQL type adapter."""

    _TYPE_MAP = {
        int: "INT",
        str: "VARCHAR(255)",
        bool: "BOOLEAN",
        float: "FLOAT",
        bytes: "LONGBLOB",
        UUID: "CHAR(36)",
        datetime: "DATETIME",
        Decimal: "DECIMAL",
    }

    def python_to_db(self, python_type: type) -> str:
        if python_type not in self._TYPE_MAP:
            raise ValueError(f"Unsupported type for MySQL: {python_type}")
        return self._TYPE_MAP[python_type]

    def is_supported(self, python_type: type) -> bool:
        return python_type in self._TYPE_MAP


@dataclass(frozen=True)
class Column[T]:
    """Column metadata for table definitions.

    Attributes:
        python_type: Python type hint (inferred from Table class annotations)
        primary_key: Whether this is a primary key
        unique: Whether values must be unique
        nullable: Whether NULL values are allowed
        default: Static default value
        default_factory: Callable that returns default value
    """

    python_type: type | None = None
    primary_key: bool = False
    unique: bool = False
    nullable: bool = False
    default: Any = None
    default_factory: Callable[[], T] | None = None

    def __post_init__(self) -> None:
        """Validate column configuration."""
        # Check conflicting defaults
        if self.default is not None and self.default_factory is not None:
            raise ValueError("Cannot specify both 'default' and 'default_factory'")

        # Check primary key with NULL
        if self.primary_key and self.nullable:
            raise ValueError("Primary key cannot be nullable")

        # Validate type is supported (only if type is set)
        if self.python_type is not None:
            self._validate_type()

    def _validate_type(self) -> None:
        """Assert type is mappable to all database types."""
        adapters = [PostgreSQLAdapter(), SQLiteAdapter(), MySQLAdapter()]
        for adapter in adapters:
            if not adapter.is_supported(self.python_type):
                raise ValueError(
                    f"Type {self.python_type} is not supported by {adapter.__class__.__name__}"
                )


class ColumnRef[T]:
    """Type-safe column reference for query building.

    Used to build WHERE clauses with operator overloading.
    """

    def __init__(self, column_name: str, python_type: type) -> None:
        assert column_name, "Column name cannot be empty"
        assert python_type, "Python type cannot be None"
        self.column_name = column_name
        self.python_type = python_type

    def __eq__(self, other: Any) -> ColumnCondition:  # type: ignore
        """Equality comparison."""
        return ColumnCondition(self.column_name, "=", other, self.python_type)

    def __ne__(self, other: Any) -> ColumnCondition:  # type: ignore
        """Inequality comparison."""
        return ColumnCondition(self.column_name, "!=", other, self.python_type)

    def __lt__(self, other: Any) -> ColumnCondition:
        """Less than comparison."""
        return ColumnCondition(self.column_name, "<", other, self.python_type)

    def __le__(self, other: Any) -> ColumnCondition:
        """Less than or equal comparison."""
        return ColumnCondition(self.column_name, "<=", other, self.python_type)

    def __gt__(self, other: Any) -> ColumnCondition:
        """Greater than comparison."""
        return ColumnCondition(self.column_name, ">", other, self.python_type)

    def __ge__(self, other: Any) -> ColumnCondition:
        """Greater than or equal comparison."""
        return ColumnCondition(self.column_name, ">=", other, self.python_type)

    def like(self, pattern: str) -> ColumnCondition:
        """SQL LIKE pattern matching."""
        assert isinstance(pattern, str), "LIKE pattern must be string"
        return ColumnCondition(self.column_name, "LIKE", pattern, self.python_type)

    def in_(self, values: list[T]) -> ColumnCondition:
        """SQL IN clause."""
        assert values, "IN list cannot be empty"
        assert all(type(v) == self.python_type for v in values), (
            f"All values in IN list must be type {self.python_type}"
        )
        return ColumnCondition(self.column_name, "IN", values, self.python_type)

    def is_null(self) -> ColumnCondition:
        """SQL IS NULL check."""
        return ColumnCondition(self.column_name, "IS NULL", None, self.python_type)

    def is_not_null(self) -> ColumnCondition:
        """SQL IS NOT NULL check."""
        return ColumnCondition(self.column_name, "IS NOT NULL", None, self.python_type)


@dataclass(frozen=True)
class ColumnCondition:
    """Represents a SQL condition for WHERE clauses."""

    column_name: str
    operator: str
    value: Any
    python_type: type

    def __and__(self, other: ColumnCondition) -> CompoundCondition:
        """AND operator."""
        return CompoundCondition([self, other], "AND")

    def __or__(self, other: ColumnCondition) -> CompoundCondition:
        """OR operator."""
        return CompoundCondition([self, other], "OR")


@dataclass(frozen=True)
class CompoundCondition:
    """Represents compound SQL conditions (AND/OR)."""

    conditions: list[ColumnCondition | CompoundCondition]
    operator: str

    def __and__(self, other: ColumnCondition | CompoundCondition) -> CompoundCondition:
        """AND operator."""
        if self.operator == "AND" and isinstance(other, ColumnCondition):
            return CompoundCondition(self.conditions + [other], "AND")
        return CompoundCondition([self, other], "AND")

    def __or__(self, other: ColumnCondition | CompoundCondition) -> CompoundCondition:
        """OR operator."""
        if self.operator == "OR" and isinstance(other, ColumnCondition):
            return CompoundCondition(self.conditions + [other], "OR")
        return CompoundCondition([self, other], "OR")
