from typing import Dict, Any

import aiomysql

from src.query_builder.core.query_result import QueryResult


class DBWorker:
    def __init__(self, connection: aiomysql.Connection):
        self._connection: aiomysql.Connection = connection
        self._jobs: int = 0

    async def query(self, sql: str) -> Dict[str, Any]:
        self.start_job()
        try:
            result = await self.execute_query(sql)
            self.end_job()
            return self.handle_result(result)
        except Exception as e:
            self.end_job()
            return self.handle_exception(e)

    async def execute_query(self, sql: str) -> QueryResult:
        async with self._connection.cursor() as cursor:
            await cursor.execute(sql)
            result_rows = await cursor.fetchall()
            result_fields = [desc[0] for desc in cursor.description]
            insert_id = cursor.lastrowid
            affected_rows = cursor.rowcount

            return QueryResult(
                insert_id=insert_id,
                affected_rows=affected_rows,
                result_fields=result_fields,
                result_rows=result_rows,
            )

    def handle_result(self, result: QueryResult) -> Dict[str, Any]:
        if result.result_rows is not None:
            return {
                'result': True,
                'count': len(result.result_rows),
                'rows': result.result_rows
            }

        res: Dict[str, Any] = {
            'result': True,
            'affectedRows': result.affected_rows
        }

        if result.insert_id != 0:
            res['insertId'] = result.insert_id

        return res

    def handle_exception(self, exception: Exception) -> Dict[str, Any]:
        return {
            'result': False,
            'error': str(exception)
        }

    def start_job(self) -> None:
        self._jobs += 1

    def end_job(self) -> None:
        self._jobs -= 1

    def get_connection(self) -> aiomysql.Connection:
        return self._connection

    def get_jobs(self) -> int:
        return self._jobs