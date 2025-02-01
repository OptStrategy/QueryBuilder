from typing import Self

from ..exceptions.query_builder_exception import QueryBuilderException
from ..utils.escape import Escape


class Table(Escape):
    """Class to manage the SQL table for queries."""

    _update_table: str

    def table(self, table: str) -> Self:
        """
        Set the table name for SQL operations.

        :param table: Name of the table to be set.
        :raises QueryBuilderException: If the table name is empty or invalid.
        :return: self, for chaining purposes.
        """
        if not table.strip():
            raise QueryBuilderException("Table is required")

        self._update_table = self._key_escape(table)
        return self

    # @property
    # def update_table(self):
    #     """Getter for the table name."""
    #     return self.__update_table
    #
    # @update_table.setter
    # def update_table(self, value):
    #     """Setter for the table name."""
    #     raise AttributeError("Direct assignment to `update_table` is not allowed; use `table` method instead.")

