from .db_result import DBResult
from ..exceptions.db_factory_exception import DBFactoryException


class EQuery:
    def __init__(self, query: str, factory):
        self.query = query
        self.factory = factory

    async def commit(self) -> DBResult:
        try:
            result = await self.factory.query(self.query)
            return result
        except DBFactoryException as e:
            return DBResult(
                is_success=False,
                message=str(e)
            )

    async def get_query(self) -> DBResult:
        return DBResult(
            is_success=True,
            message=self.query
        )
