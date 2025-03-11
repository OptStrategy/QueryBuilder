import pytest
from src.query_builder.capabilities.table import Table
from src.query_builder.exceptions.query_builder_exception import QueryBuilderException

class TestTableCapability:
    """Test suite for Table capability."""

    def test_table_initialization(self):
        """Test that Table capability initializes correctly."""
        table = Table()
        # The _update_table attribute is initialized when table() is called
        table.table('test')  # Initialize the attribute
        assert hasattr(table, '_update_table')

    def test_table_name_setting(self):
        """Test setting a valid table name."""
        table = Table()
        result = table.table('users')
        assert isinstance(result, Table)  # Test method chaining
        assert hasattr(table, '_update_table')
        assert table._update_table == '`users`'  # Test proper escaping

    def test_table_empty_name(self):
        """Test that empty table name raises exception."""
        table = Table()
        with pytest.raises(QueryBuilderException) as exc_info:
            table.table('')
        assert str(exc_info.value) == 'Table is required'

    def test_table_whitespace_name(self):
        """Test that whitespace table name raises exception."""
        table = Table()
        with pytest.raises(QueryBuilderException) as exc_info:
            table.table('   ')
        assert str(exc_info.value) == 'Table is required'

    def test_table_with_special_characters(self):
        """Test table name with special characters is properly escaped."""
        table = Table()
        result = table.table('my-table')
        assert table._update_table == '`my-table`'

    def test_table_method_chaining(self):
        """Test that table method supports chaining."""
        table = Table()
        # Should return self for method chaining
        assert table.table('users') is table 