from typing import Dict, List, Any

from ..capabilities.into import Into
from ..core.builder import Builder
from ..core.e_query import EQuery
from ..core.query import Query
from ..exceptions.query_builder_exception import QueryBuilderException


class InsertUpdate(Into):
    def __init__(self, factory=None):
        Into.__init__(self)

        self._columns: List[str] = []
        self._row: List[Any] = []
        self._updates: Dict = {}

        self._factory = factory

    def set_columns(self, columns, escape_key=True):
        if len(self._columns) > 0:
            raise QueryBuilderException("Columns already set")

        if len(self._row) != 0:
            raise QueryBuilderException("Instance has some rows, so columns can't change")

        if escape_key:
            columns = [
                self._key_escape(column_value) if escape_key else column_value
                for column_value in columns
            ]

        self._columns = columns
        return self

    def set_row(self, row, escape_value=True):
        if len(self._columns) == 0:
            raise QueryBuilderException("Columns not set")

        if len(self._row) > 0:
            raise QueryBuilderException("Row already set")

        if len(row) != len(self._columns):
            raise QueryBuilderException("Columns and row must have the same count")

        if escape_value:
            row = [self._escape(value) for value in row]

        self._row = row
        return self

    def set_update(self, column, value, escape_value=True, escape_key=True):
        if len(self._columns) == 0:
            raise QueryBuilderException("Columns not set")

        if len(self._row) == 0:
            raise QueryBuilderException("Row not set")

        if escape_key:
            column = self._key_escape(column)

        if escape_value:
            value = self._escape(value)

        self._updates[column] = value
        return self

    def set_updates(self, updates: Dict[str, Any], escape_values=True, escape_keys=True):
        for update_key, update_value in updates.items():
            self.set_update(update_key, update_value, escape_values, escape_keys)

        return self

    def compile(self):
        if not self._into_table or not self._into_table.strip():
            raise QueryBuilderException("Table required")

        if len(self._columns) == 0:
            raise QueryBuilderException("Columns required")

        if len(self._row) == 0:
            raise QueryBuilderException("Rows required")

        if len(self._updates) == 0:
            raise QueryBuilderException("Updates required")

        base_query = Builder.set_insert_table(self._into_table)
        base_query += Builder.set_insert_columns(self._columns)
        base_query += Builder.set_insert_rows([self._row])
        base_query += Builder.set_on_duplicate_key_update(self._updates)

        if self._factory is None:
            return Query(base_query)

        return EQuery(base_query, self._factory)
