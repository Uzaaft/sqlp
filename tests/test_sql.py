"""Tests for SQL generation and query building."""

import pytest
from datetime import datetime

from sqlp.sql import (
    SelectQueryBuilder,
    ConditionCompiler,
    SelectClause,
    JoinClause,
    WhereClause,
    OrderByClause,
    LimitClause,
    OffsetClause,
)
from sqlp.table import Table
from sqlp.types import Column


class TestConditionCompiler:
    """Test SQL condition compilation."""

    def test_simple_equality_postgresql(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        cond = User.email == "test@example.com"
        sql = compiler.compile(cond)
        
        assert "email" in sql
        assert "=" in sql
        assert "$1" in sql
        assert compiler.parameters == ["test@example.com"]

    def test_simple_equality_sqlite(self) -> None:
        compiler = ConditionCompiler("sqlite")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        cond = User.email == "test@example.com"
        sql = compiler.compile(cond)
        
        assert "email" in sql
        assert "=" in sql
        assert "?" in sql
        assert compiler.parameters == ["test@example.com"]

    def test_comparison_operators(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            age = Column[int]()

        # Test all comparison operators
        conditions = [
            (User.age > 18, ">"),
            (User.age < 65, "<"),
            (User.age >= 18, ">="),
            (User.age <= 65, "<="),
            (User.age != 21, "!="),
        ]
        
        for cond, op in conditions:
            compiler = ConditionCompiler("postgresql")  # Reset
            sql = compiler.compile(cond)
            assert op in sql
            assert "$1" in sql

    def test_like_operator(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        cond = User.email.like("%@gmail.com")
        sql = compiler.compile(cond)
        
        assert "LIKE" in sql
        assert "%@gmail.com" in compiler.parameters

    def test_in_operator(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            status = Column[str]()

        cond = User.status.in_(["active", "pending", "archived"])
        sql = compiler.compile(cond)
        
        assert "IN" in sql
        assert "$1" in sql
        assert "$2" in sql
        assert "$3" in sql
        assert compiler.parameters == ["active", "pending", "archived"]

    def test_is_null(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            deleted_at = Column[datetime](nullable=True)

        cond = User.deleted_at.is_null()
        sql = compiler.compile(cond)
        
        assert "IS NULL" in sql
        assert len(compiler.parameters) == 0

    def test_is_not_null(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            deleted_at = Column[datetime](nullable=True)

        cond = User.deleted_at.is_not_null()
        sql = compiler.compile(cond)
        
        assert "IS NOT NULL" in sql
        assert len(compiler.parameters) == 0

    def test_and_condition(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            age = Column[int]()
            status = Column[str]()

        cond = (User.age > 18) & (User.status == "active")
        sql = compiler.compile(cond)
        
        assert "AND" in sql
        assert "$1" in sql
        assert "$2" in sql
        assert compiler.parameters == [18, "active"]

    def test_or_condition(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            status = Column[str]()

        cond = (User.status == "active") | (User.status == "pending")
        sql = compiler.compile(cond)
        
        assert "OR" in sql
        assert compiler.parameters == ["active", "pending"]

    def test_complex_condition(self) -> None:
        compiler = ConditionCompiler("postgresql")
        
        class User(Table):
            id = Column[int](primary_key=True)
            age = Column[int]()
            status = Column[str]()
            email = Column[str]()

        cond = ((User.age > 18) & (User.status == "active")) | (
            User.email.like("%@admin.com")
        )
        sql = compiler.compile(cond)
        
        assert "AND" in sql
        assert "OR" in sql

    def test_unsupported_dialect(self) -> None:
        with pytest.raises(AssertionError):
            ConditionCompiler("mongodb")  # type: ignore


class TestSelectClause:
    """Test SELECT clause generation."""

    def test_single_table_select_all(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        select = SelectClause([User])
        sql = select.to_sql()
        
        assert "SELECT *" in sql
        assert "FROM user" in sql

    def test_multiple_table_select(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()

        select = SelectClause([User, Post])
        sql = select.to_sql()
        
        assert "FROM user, post" in sql

    def test_select_specific_columns(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        select = SelectClause([User], columns=["id", "email"])
        sql = select.to_sql()
        
        assert "SELECT id, email" in sql


class TestSelectQueryBuilder:
    """Test complete SELECT query building."""

    def test_simple_select(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = SelectQueryBuilder([User])
        stmt = builder.build()
        
        assert "SELECT *" in stmt.text
        assert "FROM user" in stmt.text

    def test_select_with_where(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = SelectQueryBuilder([User])
        builder.where(User.email == "test@example.com")
        stmt = builder.build()
        
        assert "WHERE" in stmt.text
        assert stmt.parameters == ["test@example.com"]

    def test_select_with_and_condition(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            age = Column[int]()
            status = Column[str]()

        builder = SelectQueryBuilder([User])
        builder.where((User.age > 18) & (User.status == "active"))
        stmt = builder.build()
        
        assert "AND" in stmt.text
        assert stmt.parameters == [18, "active"]

    def test_select_with_limit(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = SelectQueryBuilder([User])
        builder.limit(10)
        stmt = builder.build()
        
        assert "LIMIT 10" in stmt.text

    def test_select_with_offset(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = SelectQueryBuilder([User])
        builder.limit(10).offset(20)
        stmt = builder.build()
        
        assert "LIMIT 10" in stmt.text
        assert "OFFSET 20" in stmt.text

    def test_select_with_order_by(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = SelectQueryBuilder([User])
        builder.order_by("email")
        stmt = builder.build()
        
        assert "ORDER BY email ASC" in stmt.text

    def test_select_with_order_by_desc(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            created_at = Column[datetime]()

        builder = SelectQueryBuilder([User])
        builder.order_by("created_at", "DESC")
        stmt = builder.build()
        
        assert "ORDER BY created_at DESC" in stmt.text

    def test_select_with_multiple_order_by(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()
            created_at = Column[datetime]()

        builder = SelectQueryBuilder([User])
        builder.order_by("email").order_by("created_at", "DESC")
        stmt = builder.build()
        
        assert "ORDER BY email ASC" in stmt.text
        assert "ORDER BY created_at DESC" in stmt.text

    def test_select_with_join(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()
            title = Column[str]()

        builder = SelectQueryBuilder([User])
        builder.join(Post).on(Post.user_id == User.id)
        stmt = builder.build()
        
        assert "INNER JOIN post" in stmt.text
        assert "ON" in stmt.text
        assert "user_id" in stmt.text
        assert "id" in stmt.text

    def test_select_with_left_join(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()

        builder = SelectQueryBuilder([User])
        builder.join(Post, join_type="LEFT").on(Post.user_id == User.id)
        stmt = builder.build()
        
        assert "LEFT JOIN post" in stmt.text

    def test_select_with_multiple_joins(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()

        class Comment(Table):
            id = Column[int](primary_key=True)
            post_id = Column[int]()

        builder = SelectQueryBuilder([User])
        builder.join(Post).on(Post.user_id == User.id)
        builder.join(Comment).on(Comment.post_id == Post.id)
        stmt = builder.build()
        
        assert "INNER JOIN post" in stmt.text
        assert "INNER JOIN comment" in stmt.text

    def test_fluent_chain(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            age = Column[int]()
            email = Column[str]()

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()

        builder = SelectQueryBuilder([User])
        stmt = (
            builder.where(User.age > 18)
            .join(Post)
            .on(Post.user_id == User.id)
            .order_by("email")
            .limit(10)
            .offset(0)
            .build()
        )
        
        assert "WHERE" in stmt.text
        assert "JOIN" in stmt.text
        assert "ORDER BY" in stmt.text
        assert "LIMIT 10" in stmt.text
        assert "OFFSET 0" in stmt.text

    def test_select_sqlite_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = SelectQueryBuilder([User], sql_dialect="sqlite")
        builder.where(User.email == "test@example.com")
        stmt = builder.build()
        
        # SQLite uses ? for placeholders
        assert "?" in stmt.text
        assert stmt.parameters == ["test@example.com"]

    def test_select_mysql_dialect(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = SelectQueryBuilder([User], sql_dialect="mysql")
        builder.where(User.email == "test@example.com")
        stmt = builder.build()
        
        # MySQL uses %s for placeholders
        assert "%s" in stmt.text
        assert stmt.parameters == ["test@example.com"]

    def test_limit_validation(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = SelectQueryBuilder([User])
        with pytest.raises(AssertionError):
            builder.limit(0)
        
        with pytest.raises(AssertionError):
            builder.limit(-5)

    def test_offset_validation(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = SelectQueryBuilder([User])
        with pytest.raises(AssertionError):
            builder.offset(-1)

    def test_order_by_invalid_direction(self) -> None:
        class User(Table):
            id = Column[int](primary_key=True)

        builder = SelectQueryBuilder([User])
        with pytest.raises(AssertionError):
            builder.order_by("id", "INVALID")  # type: ignore
