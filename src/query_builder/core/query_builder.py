from typing import List, Union

from ..clauses.delete import Delete
from ..clauses.insert import Insert
from ..clauses.insert_update import InsertUpdate
from ..clauses.mulit_insert_update import MultiInsertUpdate
from ..clauses.select import Select
from ..clauses.update import Update


class QueryBuilder:
    def __init__(
            self,
            factory
    ):
        self._factory = factory

    def select(self) -> Select:
        """Create a new SELECT query."""
        return Select(self._factory)

    def insert(self) -> Insert:
        """Create a new INSERT query."""
        return Insert(self._factory)

    def update(self) -> Update:
        """Create a new UPDATE query."""
        return Update(self._factory)

    def insert_update(self) -> InsertUpdate:
        """Create a new INSERT ... ON DUPLICATE KEY UPDATE query."""
        return InsertUpdate(self._factory)

    def multi_insert_update(self) -> MultiInsertUpdate:
        """Create a new multi-row INSERT ... ON DUPLICATE KEY UPDATE query."""
        return MultiInsertUpdate(self._factory)

    def delete(self) -> Delete:
        """Create a new DELETE query."""
        return Delete(self._factory)
