import asyncio
from typing import List, Dict

import aiomysql

from ..core.db_worker import DBWorker
from ..exceptions.db_factory_exception import DBFactoryException
from ..query_builder import QueryBuilder


class DBFactory:
    MAX_CONNECTION_COUNT = 200

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

        self.logs: List[Dict] = []
        self.host = host
        self.db_name = db_name
        self.username = username
        self.password = password
        self.write_port = write_port
        self.read_port = read_port
        self.write_instance_count = write_instance_count
        self.read_instance_count = read_instance_count
        self.timeout = timeout
        self.charset = charset
        self.debug_mode = debug_mode
        self.write_connections: List[DBWorker] = []
        self.read_connections: List[DBWorker] = []

        # asyncio.run(self.create_connections())

    async def create_connections(self):
        """Create DB connections using aiomysql."""
        if self.write_connections or self.read_connections:
            raise DBFactoryException("Connections Already Created")

        # Create write connections
        for _ in range(self.write_instance_count):
            connection = await aiomysql.connect(
                host=self.host,
                port=self.write_port,
                user=self.username,
                password=self.password,
                db=self.db_name,
                charset=self.charset,
                autocommit=True
            )
            self.write_connections.append(DBWorker(connection))

        # Create read connections
        for _ in range(self.read_instance_count):
            connection = await aiomysql.connect(
                host=self.host,
                port=self.read_port,
                user=self.username,
                password=self.password,
                db=self.db_name,
                charset=self.charset,
                autocommit=True
            )
            self.read_connections.append(DBWorker(connection))

        print('Connected')

    def get_query_builder(self) -> QueryBuilder:
        if not self.read_connections or not self.write_connections:
            raise DBFactoryException("Connections Not Created")

        return QueryBuilder(self)

    async def query(self, query: str):
        is_write = not query.lower().startswith(('select', 'show'))
        best_connections = await self.get_best_connection()

        connection = self.write_connections[best_connections['write']] if is_write else self.read_connections[
            best_connections['read']]

        if not isinstance(connection, DBWorker):
            raise DBFactoryException("Connections Not Instance of Worker / Restart App")

        if not self.debug_mode:
            return await connection.query(query)

        start_time = asyncio.get_running_loop().time()
        result = await connection.query(query)
        end_time = asyncio.get_running_loop().time()
        self.logs.append({
            'query': query,
            'took': end_time - start_time,
            'isWrite': is_write,
            'status': result  # You can adjust this based on how you handle query results
        })

        return result

    async def get_best_connection(self) -> Dict[str, int]:
        if not self.read_connections or not self.write_connections:
            raise DBFactoryException("Connections Not Created")

        min_write_jobs = self.MAX_CONNECTION_COUNT
        min_jobs_writer_connection = -1

        for i, write_connection in enumerate(self.write_connections):
            tmp_write_jobs = write_connection.get_jobs()
            if tmp_write_jobs < min_write_jobs:
                min_write_jobs = tmp_write_jobs
                min_jobs_writer_connection = i

        min_read_jobs = self.MAX_CONNECTION_COUNT
        min_jobs_reader_connection = -1

        for s, read_connection in enumerate(self.read_connections):
            tmp_read_jobs = read_connection.get_jobs()
            if tmp_read_jobs < min_read_jobs:
                min_read_jobs = tmp_read_jobs
                min_jobs_reader_connection = s

        return {
            'write': min_jobs_writer_connection,
            'read': min_jobs_reader_connection
        }
