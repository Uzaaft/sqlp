"""Tests for INSERT/UPDATE/DELETE query builders."""

import pytest
from datetime import datetime

from sqlp.sql import InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder
from sqlp.table import Table
from sqlp.types import Column


class TestInsertQueryBuilder:
    """Test INSERT query building."""

    def test_simple_insert(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            name = Column[str]()

        builder = InsertQueryBuilder(User)
        builder.values({"id": 1, "email": "test@example.com", "name": "Alice"})
        stmt = builder.build()

        assert "INSERT INTO user" in stmt.text
        assert "id, email, name" in stmt.text
        assert "VALUES" in stmt.text
        assert stmt.parameters == [1, "test@example.com", "Alice"]

    def test_insert_multiple_rows(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = InsertQueryBuilder(User)
        builder.values(
            {"id": 1, "email": "alice@example.com"},
            {"id": 2, "email": "bob@example.com"},
        )
        stmt = builder.build()

        assert "VALUES" in stmt.text
        assert "($1, $2), ($3, $4)" in stmt.text
        assert stmt.parameters == [1, "alice@example.com", 2, "bob@example.com"]

    def test_insert_with_pydantic_model(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        # Create instance using row model
        user_model = User.__row_model__()
        user = user_model(id=1, email="test@example.com")

        builder = InsertQueryBuilder(User)
        builder.values(user)
        stmt = builder.build()

        assert "INSERT INTO user" in stmt.text
        assert stmt.parameters == [1, "test@example.com"]

    def test_insert_inconsistent_columns_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = InsertQueryBuilder(User)
        with pytest.raises(AssertionError):
            builder.values(
                {"id": 1, "email": "alice@example.com"},
                {"id": 2},  # Missing email
            )
            builder.build()

    def test_insert_empty_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = InsertQueryBuilder(User)
        with pytest.raises(AssertionError):
            builder.build()

    def test_insert_sqlite_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = InsertQueryBuilder(User, sql_dialect="sqlite")
        builder.values({"id": 1, "email": "test@example.com"})
        stmt = builder.build()

        assert "(?, ?)" in stmt.text

    def test_insert_mysql_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = InsertQueryBuilder(User, sql_dialect="mysql")
        builder.values({"id": 1, "email": "test@example.com"})
        stmt = builder.build()

        assert "(%s, %s)" in stmt.text

    def test_insert_invalid_type_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = InsertQueryBuilder(User)
        with pytest.raises(TypeError):
            builder.values("invalid")  # type: ignore


class TestUpdateQueryBuilder:
    """Test UPDATE query building."""

    def test_simple_update(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            name = Column[str]()

        builder = UpdateQueryBuilder(User)
        builder.set(email="newemail@example.com", name="Bob")
        builder.where(User.id == 1)
        stmt = builder.build()

        assert "UPDATE user" in stmt.text
        assert "SET" in stmt.text
        assert "WHERE" in stmt.text
        assert 1 in stmt.parameters
        assert "newemail@example.com" in stmt.parameters
        assert "Bob" in stmt.parameters

    def test_update_single_column(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = UpdateQueryBuilder(User)
        builder.set(email="new@example.com")
        builder.where(User.id == 1)
        stmt = builder.build()

        assert "SET email = $1" in stmt.text
        assert "WHERE id = $2" in stmt.text

    def test_update_no_where_clause(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = UpdateQueryBuilder(User)
        builder.set(email="new@example.com")
        stmt = builder.build()

        # Should work without WHERE (though generally unsafe)
        assert "UPDATE user" in stmt.text
        assert "SET" in stmt.text

    def test_update_with_condition(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            status = Column[str]()

        builder = UpdateQueryBuilder(User)
        builder.set(status="inactive")
        builder.where(User.status == "active")
        stmt = builder.build()

        assert "inactive" in stmt.parameters
        assert "active" in stmt.parameters

    def test_update_fluent_chain(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            age = Column[int]()

        stmt = (
            UpdateQueryBuilder(User)
            .set(email="new@example.com", age=30)
            .where(User.id == 1)
            .build()
        )

        assert "UPDATE user" in stmt.text
        assert "SET" in stmt.text
        assert "WHERE" in stmt.text

    def test_update_no_set_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = UpdateQueryBuilder(User)
        with pytest.raises(AssertionError):
            builder.build()

    def test_update_empty_set_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = UpdateQueryBuilder(User)
        builder.set(id=1)
        with pytest.raises(AssertionError):
            builder.set()  # type: ignore


class TestDeleteQueryBuilder:
    """Test DELETE query building."""

    def test_simple_delete(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = DeleteQueryBuilder(User)
        builder.where(User.id == 1)
        stmt = builder.build()

        assert "DELETE FROM user" in stmt.text
        assert "WHERE id = $1" in stmt.text
        assert stmt.parameters == [1]

    def test_delete_with_complex_condition(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            status = Column[str]()
            age = Column[int]()

        builder = DeleteQueryBuilder(User)
        builder.where((User.status == "inactive") & (User.age > 100))
        stmt = builder.build()

        assert "DELETE FROM user" in stmt.text
        assert "WHERE" in stmt.text
        assert "AND" in stmt.text

    def test_delete_no_where_error(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = DeleteQueryBuilder(User)
        with pytest.raises(AssertionError, match="safety"):
            builder.build()

    def test_delete_with_in_clause(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = DeleteQueryBuilder(User)
        builder.where(User.id.in_([1, 2, 3]))
        stmt = builder.build()

        assert "DELETE FROM user" in stmt.text
        assert "IN" in stmt.text
        assert stmt.parameters == [1, 2, 3]

    def test_delete_with_like(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = DeleteQueryBuilder(User)
        builder.where(User.email.like("%@oldomain.com"))
        stmt = builder.build()

        assert "DELETE FROM user" in stmt.text
        assert "LIKE" in stmt.text

    def test_delete_fluent_chain(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            status = Column[str]()

        stmt = (
            DeleteQueryBuilder(User)
            .where(User.status == "archived")
            .build()
        )

        assert "DELETE FROM user" in stmt.text
        assert "WHERE status = $1" in stmt.text

    def test_delete_sqlite_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = DeleteQueryBuilder(User, sql_dialect="sqlite")
        builder.where(User.id == 1)
        stmt = builder.build()

        assert "?" in stmt.text

    def test_delete_mysql_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = DeleteQueryBuilder(User, sql_dialect="mysql")
        builder.where(User.id == 1)
        stmt = builder.build()

        assert "%s" in stmt.text


class TestMutationIntegration:
    """Test mutation builders working together."""

    def test_insert_then_update_schema(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            verified = Column[bool](default=False)

        # Insert
        insert_stmt = InsertQueryBuilder(User).values(
            {"id": 1, "email": "test@example.com", "verified": False}
        ).build()

        # Update
        update_stmt = (
            UpdateQueryBuilder(User)
            .set(verified=True)
            .where(User.id == 1)
            .build()
        )

        # Delete
        delete_stmt = DeleteQueryBuilder(User).where(User.id == 1).build()

        assert "INSERT" in insert_stmt.text
        assert "UPDATE" in update_stmt.text
        assert "DELETE" in delete_stmt.text
