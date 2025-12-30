"""Table definitions and schema introspection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, get_type_hints
from pydantic import BaseModel, create_model
import inspect

from sqlp.types import Column, ColumnRef


@dataclass
class TableMetadata:
    """Metadata for a table schema."""

    name: str
    columns: dict[str, Column]
    primary_key: str | None = None
    row_model: type[BaseModel] | None = None

    def __post_init__(self) -> None:
        """Validate table metadata."""
        assert self.name, "Table name cannot be empty"
        assert self.columns, "Table must have at least one column"
        
        # Verify exactly one primary key
        pk_columns = [name for name, col in self.columns.items() if col.primary_key]
        assert len(pk_columns) <= 1, f"Table can have at most one primary key, found {len(pk_columns)}"
        if pk_columns:
            self.primary_key = pk_columns[0]

    def get_column(self, name: str) -> Column:
        """Get column by name."""
        if name not in self.columns:
            raise KeyError(f"Column '{name}' not found in table '{self.name}'")
        return self.columns[name]


class Table:
    """Base class for table definitions.
    
    Usage:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str](unique=True)
            name = Column[str]()
        
        # Type-safe column access (both styles work):
        User.id == 1           # ColumnRef[int]
        User.email.like("foo") # ColumnRef[str]
    """

    __table_metadata__: TableMetadata

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Extract table metadata from class definition."""
        super().__init_subclass__(**kwargs)
        
        # Extract Column definitions from class dict
        # We don't use type hints because Column type comes from Column[T] generic parameter
        columns: dict[str, Column] = {}
        for name, value in cls.__dict__.items():
            # Skip private/magic attributes and methods
            if name.startswith("_") or callable(value):
                continue
            
            # Check if this is a Column instance
            if isinstance(value, Column):
                # Type must be set via Column[T] syntax
                if value.python_type is None:
                    raise ValueError(
                        f"Column '{name}' in {cls.__name__} must specify type: "
                        f"use Column[<type>](...)  instead of Column(...)"
                    )
                columns[name] = value
        
        assert columns, f"Table {cls.__name__} must define at least one column"
        
        # Infer table name from class name (snake_case)
        table_name = _class_name_to_table_name(cls.__name__)
        
        # Create metadata
        metadata = TableMetadata(name=table_name, columns=columns)
        
        # Generate Pydantic model for row results
        field_definitions: dict[str, Any] = {}
        for col_name, col in columns.items():
            assert col.python_type is not None, f"Column {col_name} must have python_type set"
            if col.nullable:
                field_definitions[col_name] = (col.python_type | None, None)
            else:
                field_definitions[col_name] = (col.python_type, ...)
        
        row_model = create_model(
            f"{cls.__name__}Row",
            __base__=BaseModel,
            **field_definitions  # type: ignore
        )
        metadata.row_model = row_model
        
        # Store metadata on class
        cls.__table_metadata__ = metadata
        
        # Create ColumnRef instances as class attributes for type-safe queries
        for col_name, col in columns.items():
            assert col.python_type is not None, f"Column {col_name} must have python_type set"
            setattr(cls, col_name, ColumnRef(col_name, col.python_type))

    @classmethod
    def __table_name__(cls) -> str:
        """Get the table name."""
        return cls.__table_metadata__.name

    @classmethod
    def __columns__(cls) -> dict[str, Column]:
        """Get all columns."""
        return cls.__table_metadata__.columns

    @classmethod
    def __column__(cls, name: str) -> Column:
        """Get a column by name."""
        return cls.__table_metadata__.get_column(name)

    @classmethod
    def __row_model__(cls) -> type[BaseModel]:
        """Get the Pydantic model for rows."""
        model = cls.__table_metadata__.row_model
        assert model is not None, f"Row model not initialized for {cls.__name__}"
        return model

    @classmethod
    def __primary_key__(cls) -> str | None:
        """Get the primary key column name."""
        return cls.__table_metadata__.primary_key


def _class_name_to_table_name(class_name: str) -> str:
    """Convert class name to snake_case table name.
    
    Examples:
        User -> users
        UserProfile -> user_profiles
        HTTPServer -> http_server
    """
    result = []
    for i, char in enumerate(class_name):
        if char.isupper() and i > 0:
            # Add underscore before uppercase if previous char is lowercase
            # or current is followed by lowercase (handles acronyms)
            if (
                class_name[i - 1].islower()
                or (i + 1 < len(class_name) and class_name[i + 1].islower())
            ):
                result.append("_")
        result.append(char.lower())
    
    return "".join(result)
