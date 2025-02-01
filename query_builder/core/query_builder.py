from query_builder.clauses.delete import Delete
from query_builder.clauses.insert import Insert
from query_builder.clauses.insert_update import InsertUpdate
from query_builder.clauses.mulit_insert_update import MultiInsertUpdate
from query_builder.clauses.select import Select
from query_builder.clauses.update import Update


class QueryBuilder:

    def __init__(
            self,
            factory
    ):
        self._factory = factory

    def select(self) -> Select:
        return Select(self._factory)

    def insert(self) -> Insert:
        return Insert(self._factory)

    def update(self) -> Update:
        return Update(self._factory)

    def insert_update(self) -> InsertUpdate:
        return InsertUpdate(self._factory)

    def multi_insert_update(self) -> MultiInsertUpdate:
        return MultiInsertUpdate(self._factory)

    def delete(self) -> Delete:
        return Delete(self._factory)
