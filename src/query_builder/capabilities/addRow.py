from typing import List, Union, Self

from src.query_builder.exceptions.query_builder_exception import QueryBuilderException
from src.query_builder.utils.escape import Escape


class AddRow(Escape):
    """Class to manage adding rows to a table for SQL operations."""

    _columns: List[str] = []
    _rows: List[List[Union[str, bool]]] = []

    def add_row(self, row: List[Union[str, bool]], escape_value: bool = True) -> Self:
        """
        Add a single row to the table.

        :param row: A list of values representing a row to be added.
        :param escape_value: Whether to escape the values before adding them.
        :raises QueryBuilderException: If columns are not set or if row and columns count do not match.
        :return: self, for chaining purposes.
        """
        if len(self._columns) == 0:
            raise QueryBuilderException("Columns not set")

        if len(row) != len(self._columns):
            raise QueryBuilderException("Columns and Rows Must Have same counts")

        # Escape the values if required
        if escape_value:
            row = [self._escape(value) for value in row]

        self._rows.append(row)
        return self

    def add_rows(self, rows: List[List[Union[str, bool]]], escape_value: bool = True) -> Self:
        """
        Add multiple rows to the table.

        :param rows: A list of rows (each row is a list of values).
        :param escape_value: Whether to escape the values before adding them.
        :raises QueryBuilderException: If the input is not a multi-dimensional array.
        :return: self, for chaining purposes.
        """
        if len(rows) == len(rows[0]):
            raise QueryBuilderException("Array must be MultiDimensional")

        for row in rows:
            self.add_row(row, escape_value)

        return self
