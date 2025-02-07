from typing import List, Any

from ..capabilities.table import Table
from ..capabilities.where import Where
from ..core.builder import Builder
from ..core.e_query import EQuery
from ..core.query import Query
from ..exceptions.query_builder_exception import QueryBuilderException


class Update(Table, Where):
    def __init__(self, factory=None):
        Table.__init__(self)
        Where.__init__(self)

        self._updates: List[Any] = []  # TODO: maybe the list may be of type str

        self._factory = factory

    def set_update(self, column: str, update, escape=True):
        if escape:
            self._updates.append(f"{self._key_escape(column)} = {self._escape(update)}")
        else:
            self._updates.append(f"{self._key_escape(column)} = {update}")

        return self

    def set_updates(self, columns, escape=True):
        for column, update in columns.items():
            if not isinstance(column, str):
                raise QueryBuilderException("Update requires a key-value format")

            self.set_update(column, update, escape)

        return self

    def compile(self):
        if not self._update_table or not self._update_table.strip():
            raise QueryBuilderException("Table is required")

        if len(self._updates) == 0:
            raise QueryBuilderException("Updates required")

        where = Builder.where(self._where_statements)
        if len(where) == 0:
            raise QueryBuilderException("Where clause required")

        base_query = Builder.set_update_table(self._update_table)
        base_query += Builder.set_updates(self._updates)
        base_query += where

        if self._factory is None:
            return Query(base_query)

        return EQuery(base_query, self._factory)
