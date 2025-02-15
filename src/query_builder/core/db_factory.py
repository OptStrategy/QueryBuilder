import asyncio
from typing import List, Dict, Any

import aiomysql

from .db_result import DBResult
from ..core.db_worker import DBWorker
from ..exceptions.db_factory_exception import DBFactoryException
from .query_builder import QueryBuilder


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
        self._MAX_CONNECTION_COUNT = 200

        self._write_pool = None
        self._read_pool = None

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
                maxsize=self._write_instance_count
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
                maxsize=self._read_instance_count
            )

            print("Connection pools created successfully")
        except Exception as e:
            raise DBFactoryException(f"Failed to create connection pools: {e}")

    async def query(self, query: str) -> DBResult:
        """Run a query using either a write or read connection pool."""
        # Determine if the query is a write operation
        is_write = not query.lower().startswith(('select', 'show'))
        pool = self._write_pool if is_write else self._read_pool

        if not pool:
            raise DBFactoryException("Connection pools not initialized")

        async with pool.acquire() as connection:
            worker = DBWorker(connection)  # Create a DBWorker for this connection

            if not self._debug_mode:
                return await worker.query(query)

            # Debug mode
            start_time = asyncio.get_running_loop().time()
            result = await worker.query(query)
            end_time = asyncio.get_running_loop().time()

            self._logs.append({
                'query': query,
                'took': end_time - start_time,
                'isWrite': is_write,
                'status': result.is_success,
            })

            return result

    async def close_connections(self):
        """Close write and read connection pools."""
        if self._write_pool:
            self._write_pool.close()
            await self._write_pool.wait_closed()

        if self._read_pool:
            self._read_pool.close()
            await self._read_pool.wait_closed()

        print("Connection pools closed successfully")

    def get_query_builder(self) -> QueryBuilder:
        """Retrieve a query builder instance."""
        if not self._read_pool or not self._write_pool:
            raise DBFactoryException("Connection pools not created")

        return QueryBuilder(self)
