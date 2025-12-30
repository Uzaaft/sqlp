"""Tests for type system and column metadata."""

import pytest
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlp.types import (
    Column,
    ColumnRef,
    ColumnCondition,
    CompoundCondition,
    PostgreSQLAdapter,
    SQLiteAdapter,
    MySQLAdapter,
)


class TestTypeAdapters:
    """Test database-specific type adapters."""

    def test_postgresql_type_mapping(self) -> None:
        adapter = PostgreSQLAdapter()
        assert adapter.python_to_db(int) == "INTEGER"
        assert adapter.python_to_db(str) == "TEXT"
        assert adapter.python_to_db(bool) == "BOOLEAN"
        assert adapter.python_to_db(float) == "FLOAT8"
        assert adapter.python_to_db(bytes) == "BYTEA"
        assert adapter.python_to_db(UUID) == "UUID"
        assert adapter.python_to_db(datetime) == "TIMESTAMP"
        assert adapter.python_to_db(Decimal) == "NUMERIC"

    def test_sqlite_type_mapping(self) -> None:
        adapter = SQLiteAdapter()
        assert adapter.python_to_db(int) == "INTEGER"
        assert adapter.python_to_db(str) == "TEXT"
        assert adapter.python_to_db(bool) == "INTEGER"
        assert adapter.python_to_db(float) == "REAL"
        assert adapter.python_to_db(bytes) == "BLOB"
        assert adapter.python_to_db(UUID) == "TEXT"
        assert adapter.python_to_db(datetime) == "TEXT"

    def test_mysql_type_mapping(self) -> None:
        adapter = MySQLAdapter()
        assert adapter.python_to_db(int) == "INT"
        assert adapter.python_to_db(str) == "VARCHAR(255)"
        assert adapter.python_to_db(bool) == "BOOLEAN"
        assert adapter.python_to_db(float) == "FLOAT"
        assert adapter.python_to_db(bytes) == "LONGBLOB"

    def test_unsupported_type_raises(self) -> None:
        adapter = PostgreSQLAdapter()
        with pytest.raises(ValueError, match="Unsupported type"):
            adapter.python_to_db(complex)  # type: ignore

    def test_is_supported(self) -> None:
        adapter = PostgreSQLAdapter()
        assert adapter.is_supported(int)
        assert adapter.is_supported(str)
        assert not adapter.is_supported(complex)  # type: ignore


class TestColumnMetadata:
    """Test Column class and configuration."""

    def test_basic_column_creation(self) -> None:
        col = Column(int)
        assert col.python_type == int
        assert col.primary_key is False
        assert col.unique is False
        assert col.nullable is False

    def test_column_with_constraints(self) -> None:
        col = Column(int, primary_key=True, unique=True)
        assert col.primary_key is True
        assert col.unique is True

    def test_column_with_default_value(self) -> None:
        col = Column(int, default=0)
        assert col.default == 0

    def test_column_with_default_factory(self) -> None:
        col = Column(str, default_factory=lambda: "default_value")
        assert col.default_factory is not None

    def test_default_and_factory_conflict(self) -> None:
        with pytest.raises(ValueError, match="Cannot specify both"):
            Column(int, default=0, default_factory=lambda: 0)

    def test_primary_key_nullable_conflict(self) -> None:
        with pytest.raises(ValueError, match="Primary key cannot be nullable"):
            Column(int, primary_key=True, nullable=True)

    def test_unsupported_type_in_column(self) -> None:
        with pytest.raises(ValueError, match="not supported"):
            Column(complex)  # type: ignore

    def test_nullable_column(self) -> None:
        col = Column(str, nullable=True)
        assert col.nullable is True

    def test_unique_column(self) -> None:
        col = Column(str, unique=True)
        assert col.unique is True


class TestColumnRef:
    """Test type-safe column references and conditions."""

    def test_column_ref_creation(self) -> None:
        ref = ColumnRef("id", int)
        assert ref.column_name == "id"
        assert ref.python_type == int

    def test_column_name_cannot_be_empty(self) -> None:
        with pytest.raises(AssertionError):
            ColumnRef("", int)

    def test_equality_operator(self) -> None:
        ref = ColumnRef("email", str)
        cond = ref == "test@example.com"
        assert isinstance(cond, ColumnCondition)
        assert cond.column_name == "email"
        assert cond.operator == "="
        assert cond.value == "test@example.com"

    def test_inequality_operator(self) -> None:
        ref = ColumnRef("id", int)
        cond = ref != 5
        assert cond.operator == "!="
        assert cond.value == 5

    def test_comparison_operators(self) -> None:
        ref = ColumnRef("age", int)
        assert (ref < 18).operator == "<"
        assert (ref <= 18).operator == "<="
        assert (ref > 18).operator == ">"
        assert (ref >= 18).operator == ">="

    def test_like_pattern_matching(self) -> None:
        ref = ColumnRef("email", str)
        cond = ref.like("%@gmail.com")
        assert cond.operator == "LIKE"
        assert cond.value == "%@gmail.com"

    def test_like_requires_string(self) -> None:
        ref = ColumnRef("email", str)
        with pytest.raises(AssertionError):
            ref.like(123)  # type: ignore

    def test_in_clause(self) -> None:
        ref = ColumnRef("status", str)
        cond = ref.in_(["active", "pending", "archived"])
        assert cond.operator == "IN"
        assert cond.value == ["active", "pending", "archived"]

    def test_in_requires_nonempty_list(self) -> None:
        ref = ColumnRef("status", str)
        with pytest.raises(AssertionError):
            ref.in_([])

    def test_in_type_checking(self) -> None:
        ref = ColumnRef("id", int)
        with pytest.raises(AssertionError):
            ref.in_([1, "2", 3])  # type: ignore

    def test_is_null(self) -> None:
        ref = ColumnRef("deleted_at", str)
        cond = ref.is_null()
        assert cond.operator == "IS NULL"
        assert cond.value is None

    def test_is_not_null(self) -> None:
        ref = ColumnRef("deleted_at", str)
        cond = ref.is_not_null()
        assert cond.operator == "IS NOT NULL"
        assert cond.value is None


class TestColumnConditions:
    """Test condition composition with AND/OR operators."""

    def test_and_operator(self) -> None:
        ref_age = ColumnRef("age", int)
        ref_name = ColumnRef("name", str)
        cond = (ref_age > 18) & (ref_name == "Alice")
        assert isinstance(cond, CompoundCondition)
        assert cond.operator == "AND"
        assert len(cond.conditions) == 2

    def test_or_operator(self) -> None:
        ref_status = ColumnRef("status", str)
        cond = (ref_status == "active") | (ref_status == "pending")
        assert isinstance(cond, CompoundCondition)
        assert cond.operator == "OR"
        assert len(cond.conditions) == 2

    def test_complex_condition_chain(self) -> None:
        ref_age = ColumnRef("age", int)
        ref_status = ColumnRef("status", str)
        ref_email = ColumnRef("email", str)
        cond = ((ref_age > 18) & (ref_status == "active")) | (ref_email.like("%@admin.com"))
        assert isinstance(cond, CompoundCondition)
        assert cond.operator == "OR"

    def test_and_with_compound_condition(self) -> None:
        ref_a = ColumnRef("a", int)
        ref_b = ColumnRef("b", int)
        ref_c = ColumnRef("c", int)
        cond = (ref_a > 5) & ((ref_b < 10) & (ref_c == 15))
        assert isinstance(cond, CompoundCondition)
        assert cond.operator == "AND"
