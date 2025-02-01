from query_builder.capabilities.from_capability import From
from query_builder.capabilities.limit import Limit
from query_builder.capabilities.where import Where
from query_builder.core.builder import Builder
from query_builder.core.e_query import EQuery
from query_builder.core.query import Query
from query_builder.exceptions.query_builder_exception import QueryBuilderException


class Delete(From, Where, Limit):
    def __init__(self, factory=None):
        self._factory = factory

    def compile(self):
        if not self.from_table or not self._from_table.strip():
            raise QueryBuilderException("From is required")

        where = Builder.where(self._where_statements)
        if not where.strip():
            raise QueryBuilderException("Where is required")

        base_query = Builder.set_delete_table(self._from_table)
        base_query += where

        if self._count is not None:
            base_query += Builder.count(self._count)
            if self._offset is not None:
                base_query += Builder.offset(self._offset)

        # Return an EQuery if a factory is provided, else return a standard Query
        if self._factory is None:
            return Query(base_query)

        return EQuery(base_query, self._factory)
