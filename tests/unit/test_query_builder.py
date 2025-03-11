import pytest
from src.query_builder.core.query_builder import QueryBuilder
from src.query_builder.clauses.select import Select
from src.query_builder.clauses.insert import Insert
from src.query_builder.clauses.update import Update
from src.query_builder.clauses.delete import Delete
from src.query_builder.clauses.insert_update import InsertUpdate
from src.query_builder.clauses.mulit_insert_update import MultiInsertUpdate

class TestQueryBuilder:
    """Test suite for QueryBuilder class."""

    def test_query_builder_initialization(self, db_factory):
        """Test that QueryBuilder initializes correctly."""
        query_builder = QueryBuilder(db_factory)
        assert query_builder._factory == db_factory

    def test_select_returns_select_clause(self, db_factory):
        """Test that select() returns a Select clause instance."""
        query_builder = QueryBuilder(db_factory)
        select_clause = query_builder.select()
        assert isinstance(select_clause, Select)

    def test_insert_returns_insert_clause(self, db_factory):
        """Test that insert() returns an Insert clause instance."""
        query_builder = QueryBuilder(db_factory)
        insert_clause = query_builder.insert()
        assert isinstance(insert_clause, Insert)

    def test_update_returns_update_clause(self, db_factory):
        """Test that update() returns an Update clause instance."""
        query_builder = QueryBuilder(db_factory)
        update_clause = query_builder.update()
        assert isinstance(update_clause, Update)

    def test_delete_returns_delete_clause(self, db_factory):
        """Test that delete() returns a Delete clause instance."""
        query_builder = QueryBuilder(db_factory)
        delete_clause = query_builder.delete()
        assert isinstance(delete_clause, Delete)

    def test_insert_update_returns_insert_update_clause(self, db_factory):
        """Test that insert_update() returns an InsertUpdate clause instance."""
        query_builder = QueryBuilder(db_factory)
        insert_update_clause = query_builder.insert_update()
        assert isinstance(insert_update_clause, InsertUpdate)

    def test_multi_insert_update_returns_multi_insert_update_clause(self, db_factory):
        """Test that multi_insert_update() returns a MultiInsertUpdate clause instance."""
        query_builder = QueryBuilder(db_factory)
        multi_insert_update_clause = query_builder.multi_insert_update()
        assert isinstance(multi_insert_update_clause, MultiInsertUpdate) 