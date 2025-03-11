import pytest
import asyncio
from src.query_builder.core.db_factory import DBFactory

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def db_factory():
    """Create a DBFactory instance with actual test database configuration."""
    factory = DBFactory(
        host='localhost',
        db_name='Test',
        username='root',
        password='root',
        debug_mode=True
    )
    
    try:
        # Create connection pools
        await factory.create_connections()
        
        # Clean up the test database before each test
        await factory.query('DELETE FROM users')
        
        yield factory
    finally:
        # Clean up after tests
        try:
            await factory.query('DELETE FROM users')
        except Exception:
            pass  # Ignore cleanup errors
        finally:
            try:
                await factory.close_connections()
            except Exception:
                pass  # Ignore cleanup errors

@pytest.fixture
def mock_connection(mocker):
    """Create a mock database connection."""
    connection = mocker.MagicMock()
    cursor = mocker.MagicMock()
    connection.cursor.return_value.__aenter__.return_value = cursor
    return connection, cursor

@pytest.fixture
async def setup_test_data(db_factory):
    """Setup some test data in the database."""
    factory = await anext(db_factory)
    test_data = [
        ('John Doe', 1234567890, 'john@example.com'),
        ('Jane Smith', 9876543210, 'jane@example.com'),
        ('Bob Wilson', 5555555555, 'bob@example.com')
    ]
    
    try:
        for name, phone, email in test_data:
            query = f"""
            INSERT INTO users (name, phone, email) 
            VALUES ('{name}', {phone}, '{email}')
            """
            await factory.query(query)
        
        return factory  # Return directly instead of yielding
    except Exception as e:
        print(f"Error in setup_test_data: {e}")
        raise 