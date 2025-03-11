from typing import Dict, Any, Optional

import aiomysql

from .db_result import DBResult
from .query_result import QueryResult
from ..exceptions.db_factory_exception import DBFactoryException


class DBWorker:
    def __init__(self, connection: aiomysql.Connection):
        self._connection: aiomysql.Connection = connection
        self._jobs: int = 0  # Tracks the number of jobs, for compatibility with existing design
        self._current_transaction = None  # Remove type hint to avoid circular import

    async def query(self, sql: str) -> DBResult:
        """Execute a query and handle the result."""
        self.start_job()
        try:
            result = await self.execute_query(sql)
            self.end_job()
            return self.handle_result(result)
        except Exception as e:
            self.end_job()
            return self.handle_exception(e)

    async def execute_query(self, query: str) -> QueryResult:
        """Execute the raw query and return a QueryResult."""
        async with self._connection.cursor() as cursor:
            await cursor.execute(query)

            if cursor.description is None:
                # Non-SELECT queries (e.g., INSERT, UPDATE, DELETE)
                result_fields = None
                result_rows = None
            else:
                # SELECT queries
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
        """Process the QueryResult into a DBResult."""
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
        """Handle any exceptions that occur during query execution."""
        return DBResult(
            is_success=False,
            message=str(exception)
        )

    def start_job(self) -> None:
        """Increment the job counter."""
        self._jobs += 1

    def end_job(self) -> None:
        """Decrement the job counter."""
        self._jobs -= 1

    def get_connection(self) -> aiomysql.Connection:
        """Retrieve the underlying connection (if needed)."""
        return self._connection

    def get_jobs(self) -> int:
        """Retrieve the current job count."""
        return self._jobs

    def start_transaction(self):
        """Create and return a new transaction instance."""
        if self._current_transaction and self._current_transaction.is_active:
            raise DBFactoryException("A transaction is already active")
        
        # Import here to avoid circular import
        from .transaction import Transaction    
        self._current_transaction = Transaction(self)
        return self._current_transaction

    @property
    def has_active_transaction(self) -> bool:
        """Check if there's an active transaction."""
        return self._current_transaction is not None and self._current_transaction.is_active
