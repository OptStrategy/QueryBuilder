import pytest
from unittest.mock import patch, AsyncMock
from src.query_builder.core.db_factory import DBFactory
from src.query_builder.exceptions.db_factory_exception import DBFactoryException
from src.query_builder.core.query_builder import QueryBuilder


class TestDBFactory:
    """Unit tests for DBFactory class."""

    @pytest.fixture
    def db_config(self):
        """Return a test database configuration."""
        return {
            'host': 'localhost',
            'db_name': 'test_db',
            'username': 'test_user',
            'password': 'test_pass',
            'write_port': 3306,
            'read_port': 3306,
            'write_instance_count': 2,
            'read_instance_count': 2,
            'timeout': 2,
            'charset': 'utf8mb4',
            'debug_mode': True
        }

    def test_db_factory_initialization(self, db_config):
        """Test DBFactory initialization with valid configuration."""
        factory = DBFactory(**db_config)
        assert factory._host == db_config['host']
        assert factory._db_name == db_config['db_name']
        assert factory._username == db_config['username']
        assert factory._password == db_config['password']
        assert factory._write_port == db_config['write_port']
        assert factory._read_port == db_config['read_port']
        assert factory._write_instance_count == db_config['write_instance_count']
        assert factory._read_instance_count == db_config['read_instance_count']
        assert factory._timeout == db_config['timeout']
        assert factory._charset == db_config['charset']
        assert factory._debug_mode == db_config['debug_mode']

    @pytest.mark.asyncio
    async def test_create_connections_success(self, db_config):
        """Test successful connection pool creation."""
        with patch('aiomysql.create_pool', new_callable=AsyncMock) as mock_create_pool:
            factory = DBFactory(**db_config)
            await factory.create_connections()

            # Verify create_pool was called twice (read and write pools)
            assert mock_create_pool.call_count == 2

    @pytest.mark.asyncio
    async def test_create_connections_failure(self, db_config):
        """Test connection pool creation failure."""
        with patch('aiomysql.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")
            factory = DBFactory(**db_config)

            with pytest.raises(DBFactoryException) as exc_info:
                await factory.create_connections()

            assert "Failed to create connection pools" in str(exc_info.value)

    def test_get_query_builder_without_connections(self, db_config):
        """Test getting query builder without connections."""
        factory = DBFactory(**db_config)
        with pytest.raises(DBFactoryException) as exc_info:
            factory.get_query_builder()
        assert "Connection pools not created" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_query_builder_with_connections(self, db_config):
        """Test getting query builder with connections."""
        with patch('aiomysql.create_pool', new_callable=AsyncMock) as mock_create_pool:
            factory = DBFactory(**db_config)
            await factory.create_connections()
            query_builder = factory.get_query_builder()
            assert isinstance(query_builder, QueryBuilder)

    @pytest.mark.asyncio
    async def test_close_connections(self, db_config):
        """Test closing connection pools."""
        with patch('aiomysql.create_pool', new_callable=AsyncMock) as mock_create_pool:
            factory = DBFactory(**db_config)
            await factory.create_connections()

            # Mock the pool objects
            mock_write_pool = AsyncMock()
            mock_read_pool = AsyncMock()

            factory._write_pool = mock_write_pool
            factory._read_pool = mock_read_pool

            # Close connections
            await factory.close_connections()

            # Verify both pools were closed and waited for
            mock_write_pool.close.assert_called_once()
            mock_write_pool.wait_closed.assert_called_once()
            mock_read_pool.close.assert_called_once()
            mock_read_pool.wait_closed.assert_called_once()

            # Verify pools were set to None
            assert factory._write_pool is None
            assert factory._read_pool is None 