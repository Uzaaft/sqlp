"""SQL generation and query building."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Union

from sqlp.types import ColumnCondition, ColumnRef, CompoundCondition


@dataclass
class SQLParameter:
    """Represents a parameterized SQL value."""

    placeholder: str  # e.g., "$1" for PostgreSQL, "?" for SQLite
    value: Any


@dataclass
class SQLStatement:
    """Represents a complete SQL statement with parameters."""

    text: str
    parameters: list[Any] = field(default_factory=list)


class ConditionCompiler:
    """Compiles column conditions to SQL WHERE clauses."""

    def __init__(self, sql_dialect: str = "postgresql") -> None:
        """Initialize with a SQL dialect.
        
        Dialects:
            postgresql: $1, $2, ... for placeholders
            sqlite: ? for placeholders
            mysql: %s for placeholders
        """
        assert sql_dialect in ("postgresql", "sqlite", "mysql"), (
            f"Unsupported SQL dialect: {sql_dialect}"
        )
        self.sql_dialect = sql_dialect
        self.parameters: list[Any] = []
        self._param_counter = 0

    def _next_placeholder(self) -> str:
        """Get next parameter placeholder based on dialect."""
        if self.sql_dialect == "postgresql":
            self._param_counter += 1
            return f"${self._param_counter}"
        elif self.sql_dialect == "sqlite":
            return "?"
        elif self.sql_dialect == "mysql":
            return "%s"
        raise AssertionError("Invalid dialect")

    def compile(self, condition: Union[ColumnCondition, CompoundCondition]) -> str:
        """Compile a condition to SQL WHERE clause fragment."""
        if isinstance(condition, ColumnCondition):
            return self._compile_simple(condition)
        else:  # CompoundCondition
            return self._compile_compound(condition)

    def _compile_simple(self, condition: ColumnCondition) -> str:
        """Compile a simple column condition."""
        col_name = condition.column_name
        operator = condition.operator
        value = condition.value

        if operator == "IS NULL":
            return f"{col_name} IS NULL"
        elif operator == "IS NOT NULL":
            return f"{col_name} IS NOT NULL"
        elif operator in ("IN",):
            # Handle IN clause with multiple values
            assert isinstance(value, list), "IN operator requires a list"
            placeholders = [self._next_placeholder() for _ in value]
            self.parameters.extend(value)
            return f"{col_name} IN ({', '.join(placeholders)})"
        else:
            # Regular comparison operators
            placeholder = self._next_placeholder()
            self.parameters.append(value)
            return f"{col_name} {operator} {placeholder}"

    def _compile_compound(self, condition: CompoundCondition) -> str:
        """Compile a compound condition (AND/OR)."""
        parts = []
        for cond in condition.conditions:
            parts.append(f"({self.compile(cond)})")
        
        operator = f" {condition.operator} "
        return operator.join(parts)


@dataclass
class SelectClause:
    """Represents the SELECT clause of a query."""

    tables: list[type]
    columns: list[str] | None = None  # None means SELECT *

    def to_sql(self) -> str:
        """Generate SQL SELECT clause."""
        if self.columns is None or not self.columns:
            columns_part = "*"
        else:
            columns_part = ", ".join(self.columns)
        
        # Build table aliases for FROM clause
        table_names = []
        for table in self.tables:
            table_names.append(table.__table_name__())
        
        from_clause = ", ".join(table_names)
        return f"SELECT {columns_part} FROM {from_clause}"


@dataclass
class JoinClause:
    """Represents a JOIN clause."""

    join_type: str  # INNER, LEFT, RIGHT, FULL
    table: type
    on_condition: Union[ColumnCondition, CompoundCondition] | None = None

    def to_sql(self, compiler: ConditionCompiler) -> str:
        """Generate SQL JOIN clause."""
        table_name = self.table.__table_name__()
        sql = f"{self.join_type} JOIN {table_name}"
        
        if self.on_condition:
            on_sql = compiler.compile(self.on_condition)
            sql += f" ON {on_sql}"
        
        return sql


@dataclass
class WhereClause:
    """Represents a WHERE clause."""

    condition: Union[ColumnCondition, CompoundCondition]

    def to_sql(self, compiler: ConditionCompiler) -> str:
        """Generate SQL WHERE clause."""
        condition_sql = compiler.compile(self.condition)
        return f"WHERE {condition_sql}"


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause."""

    column_name: str
    direction: str = "ASC"  # ASC or DESC

    def to_sql(self) -> str:
        """Generate SQL ORDER BY clause."""
        assert self.direction in ("ASC", "DESC"), (
            f"Invalid order direction: {self.direction}"
        )
        return f"ORDER BY {self.column_name} {self.direction}"


@dataclass
class LimitClause:
    """Represents a LIMIT clause."""

    limit: int

    def to_sql(self) -> str:
        """Generate SQL LIMIT clause."""
        assert self.limit > 0, "LIMIT must be positive"
        return f"LIMIT {self.limit}"


@dataclass
class OffsetClause:
    """Represents an OFFSET clause."""

    offset: int

    def to_sql(self) -> str:
        """Generate SQL OFFSET clause."""
        assert self.offset >= 0, "OFFSET must be non-negative"
        return f"OFFSET {self.offset}"


class SelectQueryBuilder:
    """Builds parameterized SELECT queries."""

    def __init__(
        self,
        tables: list[type],
        sql_dialect: str = "postgresql",
    ) -> None:
        assert tables, "At least one table required"
        self.tables = tables
        self.sql_dialect = sql_dialect
        self.join_clauses: list[JoinClause] = []
        self.where_clause: WhereClause | None = None
        self.order_by_clauses: list[OrderByClause] = []
        self.limit_clause: LimitClause | None = None
        self.offset_clause: OffsetClause | None = None
        self._compiler = ConditionCompiler(sql_dialect)

    def join(
        self,
        table: type,
        join_type: str = "INNER",
    ) -> JoinBuilder:
        """Start a JOIN clause with ON condition builder."""
        assert join_type in ("INNER", "LEFT", "RIGHT", "FULL"), (
            f"Invalid join type: {join_type}"
        )
        return JoinBuilder(self, table, join_type)

    def where(
        self, condition: Union[ColumnCondition, CompoundCondition]
    ) -> SelectQueryBuilder:
        """Add WHERE clause."""
        assert condition is not None, "WHERE condition cannot be None"
        self.where_clause = WhereClause(condition)
        return self

    def order_by(
        self, column_name: str, direction: str = "ASC"
    ) -> SelectQueryBuilder:
        """Add ORDER BY clause."""
        assert column_name, "Column name cannot be empty"
        assert direction in ("ASC", "DESC"), f"Invalid direction: {direction}"
        self.order_by_clauses.append(OrderByClause(column_name, direction))
        return self

    def limit(self, limit: int) -> SelectQueryBuilder:
        """Add LIMIT clause."""
        assert limit > 0, "LIMIT must be positive"
        self.limit_clause = LimitClause(limit)
        return self

    def offset(self, offset: int) -> SelectQueryBuilder:
        """Add OFFSET clause."""
        assert offset >= 0, "OFFSET must be non-negative"
        self.offset_clause = OffsetClause(offset)
        return self

    def build(self) -> SQLStatement:
        """Build the complete SQL statement."""
        parts: list[str] = []

        # SELECT clause
        select = SelectClause(self.tables)
        parts.append(select.to_sql())

        # JOIN clauses
        for join in self.join_clauses:
            parts.append(join.to_sql(self._compiler))

        # WHERE clause
        if self.where_clause:
            parts.append(self.where_clause.to_sql(self._compiler))

        # ORDER BY clauses
        for order_by in self.order_by_clauses:
            parts.append(order_by.to_sql())

        # LIMIT clause
        if self.limit_clause:
            parts.append(self.limit_clause.to_sql())

        # OFFSET clause
        if self.offset_clause:
            parts.append(self.offset_clause.to_sql())

        sql_text = "\n".join(parts)
        return SQLStatement(sql_text, self._compiler.parameters.copy())


class JoinBuilder:
    """Builder for JOIN clauses with ON condition."""

    def __init__(
        self,
        parent: SelectQueryBuilder,
        table: type,
        join_type: str,
    ) -> None:
        self.parent = parent
        self.table = table
        self.join_type = join_type

    def on(
        self, condition: Union[ColumnCondition, CompoundCondition]
    ) -> SelectQueryBuilder:
        """Complete JOIN with ON condition."""
        assert condition is not None, "ON condition cannot be None"
        join = JoinClause(self.join_type, self.table, condition)
        self.parent.join_clauses.append(join)
        return self.parent
