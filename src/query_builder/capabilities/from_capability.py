from typing import Self

from ..exceptions.query_builder_exception import QueryBuilderException
from src.query_builder.utils.escape import Escape


class From(Escape):
    """Class to manage the FROM clause for SQL queries."""

    _from_table: str

    def from_table(self, table: str) -> Self:
        """
        Set the FROM table for SQL operations.

        :param table: The name of the table.
        :raises QueryBuilderException: If the table name is empty.
        :return: self, for chaining purposes.
        """
        if not table.strip():
            raise QueryBuilderException("from is required")

        self._from_table = self._key_escape(table)
        return self
