"""SQL generation and query building."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlp.types import ColumnCondition, CompoundCondition

if TYPE_CHECKING:
    from sqlp.table import Table


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

    def compile(self, condition: ColumnCondition | CompoundCondition) -> str:
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

    tables: list[type[Table]]
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
    table: type[Table]
    on_condition: ColumnCondition | CompoundCondition | None = None

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

    condition: ColumnCondition | CompoundCondition

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
        tables: list[type[Table]],
        sql_dialect: str = "postgresql",
        registry: Any = None,
    ) -> None:
        assert tables, "At least one table required"
        self.tables: list[type[Table]] = tables
        self.sql_dialect = sql_dialect
        self.registry = registry
        self.join_clauses: list[JoinClause] = []
        self.where_clause: WhereClause | None = None
        self.order_by_clauses: list[OrderByClause] = []
        self.limit_clause: LimitClause | None = None
        self.offset_clause: OffsetClause | None = None
        self._compiler = ConditionCompiler(sql_dialect)

        # Validate tables exist in registry if provided
        if self.registry is not None:
            for table in self.tables:
                table_name = table.__table_name__()
                if not self.registry.table_exists(table_name):
                    raise ValueError(
                        f"Table '{table_name}' not found in schema registry"
                    )

    def join(
        self,
        table: type[Table],
        join_type: str = "INNER",
    ) -> JoinBuilder:
        """Start a JOIN clause with ON condition builder."""
        assert join_type in ("INNER", "LEFT", "RIGHT", "FULL"), (
            f"Invalid join type: {join_type}"
        )
        return JoinBuilder(self, table, join_type)

    def where(
        self, condition: ColumnCondition | CompoundCondition
    ) -> SelectQueryBuilder:
        """Add WHERE clause."""
        assert condition is not None, "WHERE condition cannot be None"
        self.where_clause = WhereClause(condition)
        return self

    def order_by(self, column_name: str, direction: str = "ASC") -> SelectQueryBuilder:
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
        self._validate_if_registry()

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

    def _validate_if_registry(self) -> None:
        """Validate query against registry if provided."""
        if self.registry is None:
            return

        # Validate WHERE conditions reference existing columns
        if self.where_clause:
            self._validate_condition(self.where_clause.condition)

        # Validate JOIN conditions
        for join in self.join_clauses:
            if join.on_condition:
                self._validate_condition(join.on_condition)

        # Validate ORDER BY columns
        for order_by in self.order_by_clauses:
            # ORDER BY can reference columns from any selected table
            found = False
            for table in self.tables:
                table_name = table.__table_name__()
                if self.registry.column_exists(table_name, order_by.column_name):
                    found = True
                    break
            if not found:
                raise ValueError(
                    f"Column '{order_by.column_name}' not found in any selected table"
                )

    def _validate_condition(
        self, condition: ColumnCondition | CompoundCondition
    ) -> None:
        """Validate a condition's columns exist in schema."""
        if isinstance(condition, ColumnCondition):
            # Check if column exists in any of the selected tables
            found = False
            for table in self.tables:
                table_name = table.__table_name__()
                if self.registry.column_exists(table_name, condition.column_name):
                    found = True
                    break
            if not found:
                raise ValueError(
                    f"Column '{condition.column_name}' not found in selected tables"
                )
        else:  # CompoundCondition
            for cond in condition.conditions:
                self._validate_condition(cond)


class JoinBuilder:
    """Builder for JOIN clauses with ON condition."""

    def __init__(
        self,
        parent: SelectQueryBuilder,
        table: type[Table],
        join_type: str,
    ) -> None:
        self.parent = parent
        self.table = table
        self.join_type = join_type

    def on(self, condition: ColumnCondition | CompoundCondition) -> SelectQueryBuilder:
        """Complete JOIN with ON condition."""
        assert condition is not None, "ON condition cannot be None"
        join = JoinClause(self.join_type, self.table, condition)
        self.parent.join_clauses.append(join)
        return self.parent


class InsertQueryBuilder:
    """Builds parameterized INSERT queries."""

    def __init__(
        self,
        table: type[Table],
        sql_dialect: str = "postgresql",
        registry: Any = None,
    ) -> None:
        assert table is not None, "Table required"
        self.table: type[Table] = table
        self.sql_dialect = sql_dialect
        self.registry = registry
        self._values: list[dict[str, Any]] = []
        self._compiler = ConditionCompiler(sql_dialect)

        # Validate table exists in registry if provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            if not self.registry.table_exists(table_name):
                raise ValueError(f"Table '{table_name}' not found in schema registry")

    def values(self, *rows: Any) -> InsertQueryBuilder:
        """Add one or more rows to insert."""
        for row in rows:
            if isinstance(row, dict):
                row_dict = row
            else:
                # Assume Pydantic model
                if hasattr(row, "model_dump"):
                    row_dict = row.model_dump()  # type: ignore
                else:
                    raise TypeError(f"Expected dict or Pydantic model, got {type(row)}")

            assert row_dict, "Row cannot be empty"
            self._values.append(row_dict)

        return self

    def build(self) -> SQLStatement:
        """Build the INSERT statement."""
        assert self._values, "No values to insert"

        # Get first row to extract column names (all rows should have same columns)
        first_row = self._values[0]
        columns = list(first_row.keys())

        # Validate all rows have same columns
        for row in self._values[1:]:
            assert set(row.keys()) == set(columns), "All rows must have same columns"

        # Validate columns exist in registry if provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            for col_name in columns:
                if not self.registry.column_exists(table_name, col_name):
                    raise ValueError(
                        f"Column '{col_name}' not found in table '{table_name}'"
                    )

        table_name = self.table.__table_name__()
        columns_str = ", ".join(columns)

        # Build placeholders for each row
        parameters: list[Any] = []
        value_rows: list[str] = []

        for row in self._values:
            row_params = []
            for col in columns:
                param_placeholder = self._next_placeholder()
                row_params.append(param_placeholder)
                parameters.append(row[col])

            value_rows.append(f"({', '.join(row_params)})")

        values_str = ", ".join(value_rows)
        sql = f"INSERT INTO {table_name} ({columns_str})\nVALUES {values_str}"

        return SQLStatement(sql, parameters)

    def _next_placeholder(self) -> str:
        """Get next parameter placeholder based on dialect."""
        if self.sql_dialect == "postgresql":
            self._compiler._param_counter += 1
            return f"${self._compiler._param_counter}"
        elif self.sql_dialect == "sqlite":
            return "?"
        elif self.sql_dialect == "mysql":
            return "%s"
        raise AssertionError("Invalid dialect")


class UpdateQueryBuilder:
    """Builds parameterized UPDATE queries."""

    def __init__(
        self,
        table: type[Table],
        sql_dialect: str = "postgresql",
        registry: Any = None,
    ) -> None:
        assert table is not None, "Table required"
        self.table: type[Table] = table
        self.sql_dialect = sql_dialect
        self.registry = registry
        self._set_values: dict[str, Any] = {}
        self.where_clause: WhereClause | None = None
        self._compiler = ConditionCompiler(sql_dialect)

        # Validate table exists in registry if provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            if not self.registry.table_exists(table_name):
                raise ValueError(f"Table '{table_name}' not found in schema registry")

    def set(self, **kwargs: Any) -> UpdateQueryBuilder:
        """Set column values to update."""
        assert kwargs, "No columns to set"
        self._set_values.update(kwargs)
        return self

    def where(
        self, condition: ColumnCondition | CompoundCondition
    ) -> UpdateQueryBuilder:
        """Add WHERE clause."""
        assert condition is not None, "WHERE condition cannot be None"
        self.where_clause = WhereClause(condition)
        return self

    def build(self) -> SQLStatement:
        """Build the UPDATE statement."""
        assert self._set_values, "No columns to update"

        # Validate columns exist in registry if provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            for col_name in self._set_values.keys():
                if not self.registry.column_exists(table_name, col_name):
                    raise ValueError(
                        f"Column '{col_name}' not found in table '{table_name}'"
                    )

            # Validate WHERE condition if present
            if self.where_clause:
                self._validate_condition(self.where_clause.condition, table_name)

        table_name = self.table.__table_name__()

        # Build SET clause with placeholders
        parameters: list[Any] = []
        set_parts: list[str] = []

        for col_name, value in self._set_values.items():
            param_placeholder = self._next_placeholder()
            set_parts.append(f"{col_name} = {param_placeholder}")
            parameters.append(value)

        set_str = ", ".join(set_parts)
        sql = f"UPDATE {table_name}\nSET {set_str}"

        # Add WHERE clause if present
        if self.where_clause:
            # Need to create a new compiler for WHERE to continue placeholder count
            where_sql = self.where_clause.to_sql(self._compiler)
            sql += f"\n{where_sql}"
            parameters.extend(self._compiler.parameters)

        return SQLStatement(sql, parameters)

    def _validate_condition(
        self, condition: ColumnCondition | CompoundCondition, table_name: str
    ) -> None:
        """Validate a condition's columns exist in schema."""
        if isinstance(condition, ColumnCondition):
            if not self.registry.column_exists(table_name, condition.column_name):
                raise ValueError(
                    f"Column '{condition.column_name}' not found in table '{table_name}'"
                )
        else:  # CompoundCondition
            for cond in condition.conditions:
                self._validate_condition(cond, table_name)

    def _next_placeholder(self) -> str:
        """Get next parameter placeholder based on dialect."""
        if self.sql_dialect == "postgresql":
            self._compiler._param_counter += 1
            return f"${self._compiler._param_counter}"
        elif self.sql_dialect == "sqlite":
            return "?"
        elif self.sql_dialect == "mysql":
            return "%s"
        raise AssertionError("Invalid dialect")


class DeleteQueryBuilder:
    """Builds parameterized DELETE queries."""

    def __init__(
        self,
        table: type[Table],
        sql_dialect: str = "postgresql",
        registry: Any = None,
    ) -> None:
        assert table is not None, "Table required"
        self.table: type[Table] = table
        self.sql_dialect = sql_dialect
        self.registry = registry
        self.where_clause: WhereClause | None = None
        self._compiler = ConditionCompiler(sql_dialect)

        # Validate table exists in registry if provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            if not self.registry.table_exists(table_name):
                raise ValueError(f"Table '{table_name}' not found in schema registry")

    def where(
        self, condition: ColumnCondition | CompoundCondition
    ) -> DeleteQueryBuilder:
        """Add WHERE clause."""
        assert condition is not None, "WHERE condition cannot be None"
        self.where_clause = WhereClause(condition)
        return self

    def build(self) -> SQLStatement:
        """Build the DELETE statement."""
        assert self.where_clause is not None, (
            "DELETE requires WHERE clause (safety: no unrestricted deletes)"
        )

        # Validate WHERE condition if registry provided
        if self.registry is not None:
            table_name = self.table.__table_name__()
            self._validate_condition(self.where_clause.condition, table_name)

        table_name = self.table.__table_name__()
        sql = f"DELETE FROM {table_name}\n"

        where_sql = self.where_clause.to_sql(self._compiler)
        sql += where_sql

        return SQLStatement(sql, self._compiler.parameters.copy())

    def _validate_condition(
        self, condition: ColumnCondition | CompoundCondition, table_name: str
    ) -> None:
        """Validate a condition's columns exist in schema."""
        if isinstance(condition, ColumnCondition):
            if not self.registry.column_exists(table_name, condition.column_name):
                raise ValueError(
                    f"Column '{condition.column_name}' not found in table '{table_name}'"
                )
        else:  # CompoundCondition
            for cond in condition.conditions:
                self._validate_condition(cond, table_name)
