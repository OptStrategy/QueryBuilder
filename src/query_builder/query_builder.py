from idlelib.undo import InsertCommand
from typing import Optional

from src.query_builder.clauses.delete import Delete
from src.query_builder.clauses.insert import Insert
from src.query_builder.clauses.insert_update import InsertUpdate
from src.query_builder.clauses.mulit_insert_update import MultiInsertUpdate
from src.query_builder.clauses.select import Select
from src.query_builder.clauses.update import Update
from src.query_builder.core.db_factory import DBFactory


class QueryBuilder:

    def __init__(
            self,
            factory: Optional[DBFactory] = None
    ):
        self.factory = factory

    def select(self) -> Select:
        return Select(self.factory)

    def insert(self) -> Insert:
        return Insert(self.factory)

    def update(self) -> Update:
        return Update(self.factory)

    def insert_update(self) -> InsertUpdate:
        return InsertUpdate(self.factory)

    def multi_insert_update(self) -> MultiInsertUpdate:
        return MultiInsertUpdate(self.factory)

    def delete(self) -> Delete:
        return Delete(self.factory)
