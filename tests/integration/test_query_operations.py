import pytest
from src.query_builder.core.db_result import DBResult

pytestmark = pytest.mark.asyncio

class TestQueryOperations:
    """Integration tests for query operations."""

    async def test_insert_user(self, db_factory):
        """Test inserting a user."""
        factory = await anext(db_factory)
        
        # Create a query builder
        query_builder = factory.get_query_builder()
        
        # Build and execute the query
        result = await (
            query_builder
            .insert()
            .into('users')
            .set_columns(['name', 'phone', 'email'])
            .add_row(['John Doe', 1234567890, 'john@example.com'])
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_multiple_insert_products(self, db_factory):
        """Test inserting multiple products at once."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        # Clean up products table first using delete with where clause
        result = await (
            query_builder
            .delete()
            .from_table('products')
            .where_query('1=1')  # This matches all rows
            .compile()
            .commit()
        )
        assert result.is_success
        
        # Verify the cleanup
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .compile()
            .commit()
        )
        assert result.is_success
        assert len(result.rows) == 0
        
        result = await (
            query_builder
            .insert()
            .into('products')
            .set_columns(['name', 'price', 'category', 'stock'])
            .add_rows([
                ['Laptop', 999.99, 'Electronics', 10],
                ['Mouse', 29.99, 'Electronics', 50],
                ['Keyboard', 59.99, 'Electronics', 30],
                ['Monitor', 299.99, 'Electronics', 15]
            ])
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success
        
        # Verify the inserts
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .compile()
            .commit()
        )
        
        assert result.is_success
        assert len(result.rows) == 4

    async def test_insert_update_product(self, db_factory):
        """Test insert with ON DUPLICATE KEY UPDATE."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        # First insert
        result = await (
            query_builder
            .insert_update()
            .into('products')
            .set_columns(['name', 'price', 'category', 'stock'])
            .set_row(['Gaming Mouse', 79.99, 'Electronics', 20])
            .set_updates({
                'stock': 25,
                'price': 69.99
            })
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_select_with_join(self, db_factory):
        """Test SELECT with JOIN operations."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        # Insert test data
        await query_builder.insert().into('orders').set_columns(['user_id', 'product_id', 'quantity']).add_row([1, 1, 2]).compile().commit()
        
        # Test inner join
        result = await (
            query_builder
            .select()
            .from_table('orders')
            .add_column('orders.id')
            .add_column('users.name')
            .add_column('products.name')
            .add_column('orders.quantity')
            .inner_join('users', 'orders.user_id', 'users.id')
            .inner_join('products', 'orders.product_id', 'products.id')
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_select_with_where_conditions(self, db_factory):
        """Test SELECT with various WHERE conditions."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        # Test multiple where conditions
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .where('category', 'Electronics')
            .where_greater('price', 50)
            .where_lesser('stock', 100)
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success
        
        # Test IN condition
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .where_in('name', ['Laptop', 'Monitor'])
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_select_with_group_by(self, db_factory):
        """Test SELECT with GROUP BY and aggregations."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_column('category')
            .add_column_count('*', 'product_count')
            .add_column_sum('stock', 'total_stock')
            .add_column_average('price', 'avg_price')
            .group_by('category')
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_select_with_order_and_limit(self, db_factory):
        """Test SELECT with ORDER BY and LIMIT."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .add_order('price', 'DESC')
            .set_limit(2)
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success
        assert len(result.rows) == 2

    async def test_complex_transaction(self, db_factory):
        """Test complex transaction with multiple operations."""
        factory = await anext(db_factory)
        
        # Create transaction
        transaction = await factory.begin_transaction()
        query_builder = factory.get_query_builder()
        
        try:
            # Insert a new user
            insert_user = (
                query_builder
                .insert()
                .into('users')
                .set_columns(['name', 'phone', 'email'])
                .add_row(['Jane Smith', 9876543210, 'jane@example.com'])
                .compile()
            )
            transaction.add_query(insert_user)
            
            # Insert a new product
            insert_product = (
                query_builder
                .insert()
                .into('products')
                .set_columns(['name', 'price', 'category', 'stock'])
                .add_row(['Tablet', 499.99, 'Electronics', 5])
                .compile()
            )
            transaction.add_query(insert_product)
            
            # Create an order
            insert_order = (
                query_builder
                .insert()
                .into('orders')
                .set_columns(['user_id', 'product_id', 'quantity'])
                .add_row([1, 1, 1])
                .compile()
            )
            transaction.add_query(insert_order)
            
            # Commit all queries in the transaction
            result = await transaction.commit()
            assert result.is_success
            
        except Exception as e:
            await transaction.rollback()
            raise e

    async def test_select_with_like(self, db_factory):
        """Test SELECT with LIKE conditions."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        result = await (
            query_builder
            .select()
            .from_table('products')
            .add_all_columns()
            .where_like('name', 'Mouse', begin=True, end=True)
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_multi_insert_update(self, db_factory):
        """Test multi-row insert with ON DUPLICATE KEY UPDATE."""
        factory = await anext(db_factory)
        query_builder = factory.get_query_builder()
        
        result = await (
            query_builder
            .multi_insert_update()
            .into('products')
            .set_insert_alias('new')
            .set_columns(['name', 'price', 'category', 'stock'])
            .add_rows([
                ['Laptop', 1099.99, 'Electronics', 15],
                ['Mouse', 39.99, 'Electronics', 60],
            ])
            .add_updates({
                'stock': 'new.stock',
                'price': 'new.price'
            }, escape_value=False)
            .compile()
            .commit()
        )
        
        assert isinstance(result, DBResult)
        assert result.is_success

    async def test_select_with_where(self, db_factory, setup_test_data):
        """Test selecting users with where clause."""
        factory = await setup_test_data
        
        # Create a query builder
        query_builder = factory.get_query_builder()
        
        # Build and execute the query
        result = await query_builder.select() \
            .from_table('users') \
            .add_columns(['name', 'email']) \
            .where('name', 'John Doe') \
            .compile() \
            .commit()
        
        assert isinstance(result, DBResult)
        assert result.is_success
        assert len(result.rows) == 1
        assert result.rows[0]['name'] == 'John Doe'
        assert result.rows[0]['email'] == 'john@example.com'

    async def test_update_user(self, db_factory, setup_test_data):
        """Test updating a user."""
        factory = await setup_test_data
        
        # Create a query builder
        query_builder = factory.get_query_builder()
        
        # Build and execute the update query
        result = await query_builder.update() \
            .table('users') \
            .set_updates({'phone': 9999999999}) \
            .where('name', 'John Doe') \
            .compile() \
            .commit()
        
        assert isinstance(result, DBResult)
        assert result.is_success
        assert result.affected_rows == 1
        
        # Verify the update
        result = await query_builder.select() \
            .from_table('users') \
            .add_all_columns() \
            .where('name', 'John Doe') \
            .compile() \
            .commit()
        
        assert result.is_success
        assert len(result.rows) == 1
        assert result.rows[0]['phone'] == 9999999999

    async def test_delete_user(self, db_factory, setup_test_data):
        """Test deleting a user."""
        factory = await setup_test_data
        
        # Create a query builder
        query_builder = factory.get_query_builder()
        
        # Build and execute the delete query
        result = await query_builder.delete() \
            .from_table('users') \
            .where('name', 'John Doe') \
            .compile() \
            .commit()
        
        assert isinstance(result, DBResult)
        assert result.is_success
        
        # Verify the deletion
        result = await query_builder.select() \
            .from_table('users') \
            .add_all_columns() \
            .where('name', 'John Doe') \
            .compile() \
            .commit()
        
        assert result.is_success
        assert len(result.rows) == 0

    async def test_transaction_success(self, db_factory):
        """Test successful transaction execution."""
        factory = await anext(db_factory)
        
        # Create queries for transaction
        queries = [
            """
            INSERT INTO users (name, phone, email)
            VALUES ('Test User', 1234567890, 'test@example.com')
            """,
            """
            UPDATE users
            SET phone = 9876543210
            WHERE name = 'Test User'
            """
        ]
        
        # Execute transaction
        result = await factory.execute_transaction(queries)
        assert result.is_success
        
        # Verify the transaction results
        query_builder = factory.get_query_builder()
        result = await query_builder.select() \
            .from_table('users') \
            .add_all_columns() \
            .where('name', 'Test User') \
            .compile() \
            .commit()
        
        assert result.is_success
        assert len(result.rows) == 1
        assert result.rows[0]['phone'] == 9876543210

    async def test_transaction_rollback(self, db_factory):
        """Test transaction rollback on error."""
        factory = await anext(db_factory)
        
        # Create queries for transaction (second query will fail)
        queries = [
            """
            INSERT INTO users (name, phone, email)
            VALUES ('Test User', 1234567890, 'test@example.com')
            """,
            """
            UPDATE non_existent_table
            SET phone = 9876543210
            WHERE name = 'Test User'
            """
        ]
        
        # Execute transaction
        result = await factory.execute_transaction(queries)
        assert not result.is_success
        
        # Verify the rollback (first insert should be rolled back)
        query_builder = factory.get_query_builder()
        result = await query_builder.select() \
            .from_table('users') \
            .add_all_columns() \
            .where('name', 'Test User') \
            .compile() \
            .commit()
        
        assert result.is_success
        assert len(result.rows) == 0 