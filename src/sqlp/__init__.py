"""sqlp - Async-first, Pythonic ORM with Pydantic integration and Drizzle-like API."""

__version__ = "0.1.0"

from sqlp.table import Table
from sqlp.types import Column
from sqlp.pool import AsyncPool
from sqlp.schema import validate_schema
from sqlp.executor import ExecutableQuery, ExecutableMutation

__all__ = [
    "Table",
    "Column",
    "AsyncPool",
    "validate_schema",
    "ExecutableQuery",
    "ExecutableMutation",
]
