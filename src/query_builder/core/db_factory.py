import asyncio
from typing import List, Dict, Any

import aiomysql

from ..core.db_worker import DBWorker
from ..exceptions.db_factory_exception import DBFactoryException
from .query_builder import QueryBuilder


class DBFactory:
    _MAX_CONNECTION_COUNT = 200

    _write_connections: List[DBWorker] = []
    _read_connections: List[DBWorker] = []

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
        """Create DB connections using aiomysql."""
        if self._write_connections or self._read_connections:
            raise DBFactoryException("Connections Already Created")

        # Create write connections
        for _ in range(self._write_instance_count):
            connection = await aiomysql.connect(
                host=self._host,
                port=self._write_port,
                user=self._username,
                password=self._password,
                db=self._db_name,
                charset=self._charset,
                autocommit=True
            )
            self._write_connections.append(DBWorker(connection))

        # Create read connections
        for _ in range(self._read_instance_count):
            connection = await aiomysql.connect(
                host=self._host,
                port=self._read_port,
                user=self._username,
                password=self._password,
                db=self._db_name,
                charset=self._charset,
                autocommit=True
            )
            self._read_connections.append(DBWorker(connection))

        print('Connected')

    def get_query_builder(self) -> QueryBuilder:
        if not self._read_connections or not self._write_connections:
            raise DBFactoryException("Connections Not Created")

        return QueryBuilder(self)

    async def query(self, query: str) -> Dict[str, Any]:
        is_write = not query.lower().startswith(('select', 'show'))
        best_connections = await self.__get_best_connection()

        connection = self._write_connections[best_connections['write']] if is_write else self._read_connections[
            best_connections['read']]

        if not isinstance(connection, DBWorker):
            raise DBFactoryException("Connections Not Instance of Worker / Restart App")

        if not self._debug_mode:
            return await connection.query(query)

        start_time = asyncio.get_running_loop().time()
        result = await connection.query(query)
        end_time = asyncio.get_running_loop().time()
        self._logs.append({
            'query': query,
            'took': end_time - start_time,
            'isWrite': is_write,
            'status': result  # You can adjust this based on how you handle query results
        })

        return result

    async def __get_best_connection(self) -> Dict[str, int]:
        if not self._read_connections or not self._write_connections:
            raise DBFactoryException("Connections Not Created")

        min_write_jobs = self._MAX_CONNECTION_COUNT
        min_jobs_writer_connection = -1

        for i, write_connection in enumerate(self._write_connections):
            tmp_write_jobs = write_connection.get_jobs()
            if tmp_write_jobs < min_write_jobs:
                min_write_jobs = tmp_write_jobs
                min_jobs_writer_connection = i

        min_read_jobs = self._MAX_CONNECTION_COUNT
        min_jobs_reader_connection = -1

        for s, read_connection in enumerate(self._read_connections):
            tmp_read_jobs = read_connection.get_jobs()
            if tmp_read_jobs < min_read_jobs:
                min_read_jobs = tmp_read_jobs
                min_jobs_reader_connection = s

        return {
            'write': min_jobs_writer_connection,
            'read': min_jobs_reader_connection
        }
