from ..exceptions.db_factory_exception import DBFactoryException


class EQuery:
    def __init__(self, query: str, factory):
        self.query = query
        self.factory = factory

    async def commit(self) -> dict:
        try:
            result = await self.factory.query(self.query)
            return result
        except DBFactoryException as e:
            return {
                'result': False,
                'error': str(e)
            }

    async def get_query(self) -> dict:
        return {
            'result': True,
            'query': self.query
        }
