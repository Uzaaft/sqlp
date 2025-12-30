"""Tests for Table class and schema definition."""

import pytest
from datetime import datetime

from sqlp.table import Table, TableMetadata, _class_name_to_table_name
from sqlp.types import Column, ColumnRef


class TestTableNameConversion:
    """Test class name to table name conversion."""

    def test_simple_name(self) -> None:
        assert _class_name_to_table_name("User") == "user"
        assert _class_name_to_table_name("Post") == "post"

    def test_plural_conversion(self) -> None:
        # Note: We don't pluralize, just convert to snake_case
        assert _class_name_to_table_name("User") == "user"
        assert _class_name_to_table_name("UserProfile") == "user_profile"

    def test_multiple_words(self) -> None:
        assert _class_name_to_table_name("UserProfile") == "user_profile"
        assert _class_name_to_table_name("AccountBalance") == "account_balance"

    def test_acronyms(self) -> None:
        assert _class_name_to_table_name("HTTPServer") == "http_server"
        assert _class_name_to_table_name("URLPath") == "url_path"

    def test_consecutive_capitals(self) -> None:
        assert _class_name_to_table_name("URLRouter") == "url_router"


class TestTableDefinition:
    """Test Table class definition and metadata."""

    def test_simple_table_definition(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str](unique=True)

        assert User.__table_name__() == "user"
        columns = User.__columns__()
        assert "id" in columns
        assert "email" in columns
        assert columns["id"].primary_key is True
        assert columns["email"].unique is True

    def test_column_access_in_table(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        # Columns should be accessible as ColumnRef for type-safe queries
        assert isinstance(User.id, ColumnRef)
        assert isinstance(User.email, ColumnRef)
        assert User.id.column_name == "id"
        assert User.email.column_name == "email"

    def test_table_with_nullable_columns(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            bio = Column[str](nullable=True)

        columns = User.__columns__()
        assert columns["bio"].nullable is True

    def test_table_with_defaults(self) -> None:
        class Post(Table):
            id = Column[int](primary_key=True)
            title = Column[str]()
            created_at = Column[datetime](default_factory=datetime.now)

        columns = Post.__columns__()
        assert columns["created_at"].default_factory is not None

    def test_primary_key_detection(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            name = Column[str]()

        assert User.__primary_key__() == "id"

    def test_no_primary_key(self) -> None:
        class Log(Table):
            message = Column[str]()
            level = Column[str]()

        assert Log.__primary_key__() is None

    def test_row_model_generation(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            age = Column[int | None](nullable=True)

        model = User.__row_model__()
        
        # Should be able to instantiate with valid data
        user_data = {"id": 1, "email": "test@example.com", "age": None}
        user = model(**user_data)
        assert user.id == 1  # type: ignore[attr-defined]
        assert user.email == "test@example.com"  # type: ignore[attr-defined]
        assert user.age is None  # type: ignore[attr-defined]

    def test_row_model_validation(self) -> None:
        from pydantic import ValidationError

        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        model = User.__row_model__()
        
        # Should fail with invalid types
        with pytest.raises(ValidationError):
            model(id="not_an_int", email="test@example.com")

    def test_row_model_respects_nullable(self) -> None:
        from pydantic import ValidationError

        class User(Table):
            id = Column[int](primary_key=True)
            bio = Column[str](nullable=False)

        model = User.__row_model__()
        
        # Should fail when required field is None
        with pytest.raises(ValidationError):
            model(id=1, bio=None)

    def test_multiple_tables_independent(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            name = Column[str]()

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()
            title = Column[str]()

        assert User.__table_name__() == "user"
        assert Post.__table_name__() == "post"
        assert len(User.__columns__()) == 2
        assert len(Post.__columns__()) == 3

    def test_column_refs_are_independent(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        class Post(Table):
            id = Column[int](primary_key=True)

        # Each table should have its own ColumnRef instances
        assert User.id is not Post.id
        assert User.id.column_name == "id"  # type: ignore[attr-defined]
        assert Post.id.column_name == "id"  # type: ignore[attr-defined]


class TestTableMetadata:
    """Test TableMetadata class."""

    def test_metadata_creation(self) -> None:
        columns: dict[str, Column] = {
            "id": Column(int, primary_key=True),
            "name": Column(str),
        }
        meta = TableMetadata(name="users", columns=columns)
        
        assert meta.name == "users"
        assert meta.primary_key == "id"

    def test_get_column(self) -> None:
        columns: dict[str, Column] = {
            "id": Column(int, primary_key=True),
            "email": Column(str, unique=True),
        }
        meta = TableMetadata(name="users", columns=columns)
        
        col = meta.get_column("email")
        assert col.unique is True

    def test_get_nonexistent_column(self) -> None:
        columns: dict[str, Column] = {"id": Column(int, primary_key=True)}
        meta = TableMetadata(name="users", columns=columns)
        
        with pytest.raises(KeyError):
            meta.get_column("nonexistent")

    def test_multiple_primary_keys_error(self) -> None:
        columns: dict[str, Column] = {
            "id1": Column(int, primary_key=True),
            "id2": Column(int, primary_key=True),
        }
        with pytest.raises(AssertionError):
            TableMetadata(name="users", columns=columns)

    def test_empty_table_error(self) -> None:
        with pytest.raises(AssertionError):
            TableMetadata(name="users", columns={})

    def test_empty_name_error(self) -> None:
        columns: dict[str, Column] = {"id": Column(int)}
        with pytest.raises(AssertionError):
            TableMetadata(name="", columns=columns)
