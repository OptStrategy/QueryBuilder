from typing import Dict, Any

import aiomysql

from .db_result import DBResult
from ..core.query_result import QueryResult


class DBWorker:
    def __init__(self, connection: aiomysql.Connection):
        self._connection: aiomysql.Connection = connection
        self._jobs: int = 0

    async def query(self, sql: str) -> DBResult:
        self.start_job()
        try:
            result = await self.execute_query(sql)
            self.end_job()
            return self.handle_result(result)
        except Exception as e:
            self.end_job()
            return self.handle_exception(e)

    async def execute_query(self, query: str) -> QueryResult:
        async with self._connection.cursor() as cursor:
            await cursor.execute(query)
            if cursor.description is None:
                # For INSERT queries, no result fields or rows are returned
                result_fields = None
                result_rows = None
            else:
                # For SELECT queries, cursor.description contains column info
                result_fields = [desc[0] for desc in cursor.description]
                result_rows = await cursor.fetchall()

            insert_id = cursor.lastrowid
            affected_rows = cursor.rowcount

            return QueryResult(
                insert_id=insert_id,
                affected_rows=affected_rows,
                result_fields=result_fields,
                result_rows=result_rows,
            )

    def handle_result(self, result: QueryResult) -> DBResult:
        if result.result_rows is not None:
            def map_to_dict(row):
                return {result.result_fields[i]: value for i, value in enumerate(row)}

            result_as_dicts = [map_to_dict(row) for row in result.result_rows]
            return DBResult(
                is_success=True,
                rows=result_as_dicts,
                count=len(result.result_rows)
            )

        return DBResult(
            is_success=True,
            affected_rows=result.affected_rows,
            insert_id=result.insert_id if result.insert_id is not None else None
        )

    def handle_exception(self, exception: Exception) -> DBResult:
        return DBResult(
            is_success=False,
            message=str(exception)
        )

    def start_job(self) -> None:
        self._jobs += 1

    def end_job(self) -> None:
        self._jobs -= 1

    def get_connection(self) -> aiomysql.Connection:
        return self._connection

    def get_jobs(self) -> int:
        return self._jobs
