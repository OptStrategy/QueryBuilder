from typing import List

from ..capabilities.addRow import AddRow
from ..capabilities.into import Into
from ..core.builder import Builder
from ..core.e_query import EQuery
from ..core.query import Query
from ..exceptions.query_builder_exception import QueryBuilderException


class Insert(Into, AddRow):
    def __init__(self, factory=None):
        Into.__init__(self)
        AddRow.__init__(self)

        self._factory = factory

    def set_columns(self, columns: List[str], escape_key=True):
        if len(self._rows) != 0:
            raise QueryBuilderException("Instance has some rows, so columns can't change")

        self._columns = [
            self._key_escape(column_value) if escape_key else column_value
            for column_value in columns
        ]
        return self

    def compile(self):
        if not self._into_table or not self._into_table.strip():
            raise QueryBuilderException("Table Required")

        if len(self._columns) == 0:
            raise QueryBuilderException("Columns Required")

        if len(self._rows) == 0:
            raise QueryBuilderException("Rows Required")

        base_query = Builder.set_insert_table(self._into_table)
        base_query += Builder.set_insert_columns(self._columns)
        base_query += Builder.set_insert_rows(self._rows)

        if self._factory is None:
            return Query(base_query)

        return EQuery(base_query, self._factory)
