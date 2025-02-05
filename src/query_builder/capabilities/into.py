from typing import Self

from ..exceptions.query_builder_exception import QueryBuilderException
from ..utils.escape import Escape


class Into(Escape):
    """Class to manage INTO clause for SQL queries."""

    _into_table: str

    def into(self, table: str) -> Self:
        """
        Set the INTO table for SQL operations.

        :param table: The name of the table.
        :raises QueryBuilderException: If the table name is empty.
        :return: self, for chaining purposes.
        """
        if not table.strip():
            raise QueryBuilderException("Into is required")

        self._into_table = self._key_escape(table)
        return self
