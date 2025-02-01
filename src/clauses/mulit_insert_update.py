from typing import List, Any, Dict

from ..capabilities.addRow import AddRow
from ..capabilities.into import Into
from ..core.builder import Builder
from ..core.e_query import EQuery
from ..core.query import Query
from ..exceptions.query_builder_exception import QueryBuilderException


class MultiInsertUpdate(Into, AddRow):
    _alias: str
    _rows: List[Any] = []  # TODO: maybe the type of the list must be str
    _updates: Dict[str, Any] = []

    def __init__(self, factory = None):
        self._factory = factory

    def set_columns(self, columns, escape_key=True):
        if len(self._rows) != 0:
            raise QueryBuilderException("Instance has some rows, so columns can't change")

        if escape_key:
            columns = {column_name: self._key_escape(column_value) for column_name, column_value in columns.items()}

        self._columns = columns
        return self

    def set_insert_alias(self, alias_name, escape=True):
        self._alias = self._key_escape(alias_name) if escape else alias_name
        return self

    def add_update(self, key, value, escape_value=True, escape_key=True):
        if len(self._columns) == 0:
            raise QueryBuilderException("Columns not set")

        if len(self._rows) == 0:
            raise QueryBuilderException("Rows not set")

        key = self._key_escape(key) if escape_key else key
        value = self._escape(value) if escape_value or isinstance(value, (bool, type(None))) else value

        self._updates[key] = value
        return self

    def add_updates(self, updates, escape_value=True, escape_key=True):
        for update_key, update_value in updates.items():
            self.add_update(update_key, update_value, escape_value, escape_key)
        return self

    def compile(self):
        if not self._into_table or not self._into_table.strip():
            raise QueryBuilderException("Table required")

        if not self._alias or not self._alias.strip():
            raise QueryBuilderException("Alias required")

        if len(self._columns) == 0:
            raise QueryBuilderException("Columns required")

        if len(self._rows) == 0:
            raise QueryBuilderException("Rows required")

        if len(self._updates) == 0:
            raise QueryBuilderException("Updates required")

        base_query = Builder.set_insert_table(self._into_table)
        base_query += Builder.set_insert_columns(self._columns)
        base_query += Builder.set_insert_rows(self._rows)
        base_query += Builder.as_alias(self._alias)
        base_query += Builder.set_on_duplicate_key_update(self._updates)

        return Query(base_query) if self._factory is None else EQuery(base_query, self._factory)
