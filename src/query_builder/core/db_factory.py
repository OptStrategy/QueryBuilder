import asyncio
from typing import List, Dict, Any, Optional

import aiomysql

from .db_result import DBResult
from ..core.db_worker import DBWorker
from ..exceptions.db_factory_exception import DBFactoryException
from .query_builder import QueryBuilder
from .transaction import Transaction


class DBFactory:
    def __init__(
            self,
            host: str,
            db_name: str,
            username: str,
            password: str,
            write_port: int = 3306,
            read_port: int = 3306,
            write_instance_count: int = 2,
            read_instance_count: int = 2,
            timeout: int = 2,
            charset: str = 'utf8mb4',
            debug_mode: bool = False
    ):
        if write_instance_count > 200 or read_instance_count > 200:
            raise DBFactoryException("Maximum connection count exceeded (200)")

        self._MAX_CONNECTION_COUNT = 200
        self._write_pool: Optional[aiomysql.Pool] = None
        self._read_pool: Optional[aiomysql.Pool] = None

        self._logs: List[Dict] = []
        self._host = host
        self._db_name = db_name
        self._username = username
        self._password = password
        self._write_port = write_port
        self._read_port = read_port
        self._write_instance_count = write_instance_count
        self._read_instance_count = read_instance_count
        self._timeout = timeout
        self._charset = charset
        self._debug_mode = debug_mode

    async def create_connections(self):
        """Create connection pools for write and read operations."""
        if self._write_pool or self._read_pool:
            raise DBFactoryException("Connection pools already created")

        try:
            # Create write connection pool
            self._write_pool = await aiomysql.create_pool(
                host=self._host,
                port=self._write_port,
                user=self._username,
                password=self._password,
                db=self._db_name,
                charset=self._charset,
                autocommit=True,
                maxsize=self._write_instance_count,
                minsize=1,
                pool_recycle=3600
            )

            # Create read connection pool
            self._read_pool = await aiomysql.create_pool(
                host=self._host,
                port=self._read_port,
                user=self._username,
                password=self._password,
                db=self._db_name,
                charset=self._charset,
                autocommit=True,
                maxsize=self._read_instance_count,
                minsize=1,
                pool_recycle=3600
            )

            print("Connection pools created successfully")
        except Exception as e:
            # Clean up if one pool was created but the other failed
            if self._write_pool:
                self._write_pool.close()
                await self._write_pool.wait_closed()
            if self._read_pool:
                self._read_pool.close()
                await self._read_pool.wait_closed()
            raise DBFactoryException(f"Failed to create connection pools: {e}")

    async def query(self, query: str) -> DBResult:
        """Run a query using either a write or read connection pool."""
        # Determine if the query is a write operation
        is_write = not query.lower().strip().startswith(('select', 'show'))
        pool = self._write_pool if is_write else self._read_pool

        if not pool:
            raise DBFactoryException("Connection pools not initialized")

        async with pool.acquire() as connection:
            worker = DBWorker(connection)

            if not self._debug_mode:
                return await worker.query(query)

            # Debug mode
            start_time = asyncio.get_running_loop().time()
            try:
                result = await worker.query(query)
                end_time = asyncio.get_running_loop().time()

                self._logs.append({
                    'query': query,
                    'took': end_time - start_time,
                    'isWrite': is_write,
                    'status': result.is_success,
                })

                return result
            except Exception as e:
                end_time = asyncio.get_running_loop().time()
                self._logs.append({
                    'query': query,
                    'took': end_time - start_time,
                    'isWrite': is_write,
                    'status': False,
                    'error': str(e)
                })
                raise

    async def close_connections(self):
        """Close write and read connection pools."""
        try:
            if self._write_pool:
                self._write_pool.close()
                await self._write_pool.wait_closed()
                self._write_pool = None

            if self._read_pool:
                self._read_pool.close()
                await self._read_pool.wait_closed()
                self._read_pool = None

            print("Connection pools closed successfully")
        except Exception as e:
            raise DBFactoryException(f"Failed to close connection pools: {e}")

    def get_query_builder(self) -> QueryBuilder:
        """Retrieve a query builder instance."""
        if not self._read_pool or not self._write_pool:
            raise DBFactoryException("Connection pools not created")

        return QueryBuilder(self)

    async def begin_transaction(self) -> Transaction:
        """Start a new transaction using a write connection."""
        if not self._write_pool:
            raise DBFactoryException("Write connection pool not initialized")

        connection = None
        try:
            connection = await self._write_pool.acquire()
            worker = DBWorker(connection)
            transaction = worker.start_transaction()
            await transaction.begin()
            return transaction
        except Exception as e:
            if connection:
                self._write_pool.release(connection)
            raise DBFactoryException(f"Failed to start transaction: {e}")

    async def execute_transaction(self, queries: List[str]) -> DBResult:
        """Execute multiple queries in a transaction."""
        transaction = None
        try:
            transaction = await self.begin_transaction()
            
            for query in queries:
                result = await transaction._worker.query(query)
                if not result.is_success:
                    await transaction.rollback()
                    return DBResult(
                        is_success=False,
                        message=f"Transaction failed: {result.message}"
                    )
            
            return await transaction.commit()
            
        except Exception as e:
            if transaction and transaction.is_active:
                await transaction.rollback()
            return DBResult(
                is_success=False,
                message=f"Transaction failed: {str(e)}"
            )
