from typing import Optional, List, Union
from .db_result import DBResult
from .query import Query
from .e_query import EQuery
from ..exceptions.db_factory_exception import DBFactoryException


class Transaction:
    def __init__(self, worker):
        self._worker = worker
        self._queries: List[Union[Query, EQuery]] = []
        self._is_active: bool = False
        self._is_committed: bool = False
        self._is_rolled_back: bool = False
        self._error: Optional[str] = None

    async def begin(self) -> DBResult:
        """Start a new transaction."""
        if self._is_active:
            return DBResult(
                is_success=False,
                message="Transaction already started"
            )

        try:
            result = await self._worker.query("START TRANSACTION")
            if result.is_success:
                self._is_active = True
                self._is_committed = False
                self._is_rolled_back = False
                self._error = None
            return result
        except Exception as e:
            self._error = str(e)
            return DBResult(
                is_success=False,
                message=f"Failed to start transaction: {str(e)}"
            )

    async def commit(self) -> DBResult:
        """Commit the current transaction."""
        if not self._is_active:
            return DBResult(
                is_success=False,
                message="No active transaction to commit"
            )

        try:
            # Execute all queued queries
            for query in self._queries:
                try:
                    # Handle both Query and EQuery objects
                    if isinstance(query, Query):
                        query_str = query.get_query()
                    else:  # EQuery
                        query_result = await query.get_query()
                        if not query_result.is_success:
                            await self.rollback()
                            return query_result
                        query_str = query_result.message

                    result = await self._worker.query(query_str)
                    if not result.is_success:
                        await self.rollback()
                        return result
                except Exception as e:
                    await self.rollback()
                    self._error = str(e)
                    return DBResult(
                        is_success=False,
                        message=f"Query execution failed: {str(e)}"
                    )

            # If all queries succeeded, commit the transaction
            result = await self._worker.query("COMMIT")
            if result.is_success:
                self._is_active = False
                self._is_committed = True
                self._queries.clear()
                self._error = None

            return result
        except Exception as e:
            self._error = str(e)
            await self.rollback()
            return DBResult(
                is_success=False,
                message=f"Commit failed: {str(e)}"
            )

    async def rollback(self) -> DBResult:
        """Rollback the current transaction."""
        if not self._is_active:
            return DBResult(
                is_success=False,
                message="No active transaction to rollback"
            )

        try:
            result = await self._worker.query("ROLLBACK")
            if result.is_success:
                self._is_active = False
                self._is_rolled_back = True
                self._queries.clear()
            return result
        except Exception as e:
            self._error = str(e)
            self._is_active = False  # Force inactive state on error
            self._is_rolled_back = True
            self._queries.clear()
            return DBResult(
                is_success=False,
                message=f"Rollback failed: {str(e)}"
            )

    def add_query(self, query: Union[Query, EQuery]) -> None:
        """Add a query to the transaction queue."""
        if not self._is_active:
            raise DBFactoryException("Cannot add query - no active transaction")
        if self._is_committed or self._is_rolled_back:
            raise DBFactoryException("Cannot add query - transaction already finished")
        self._queries.append(query)

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def is_committed(self) -> bool:
        return self._is_committed

    @property
    def is_rolled_back(self) -> bool:
        return self._is_rolled_back

    @property
    def error(self) -> Optional[str]:
        return self._error 