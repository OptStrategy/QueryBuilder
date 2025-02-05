from typing import List, Optional, Self

from ..enums.join_direction import JoinDirection
from ..exceptions.query_builder_exception import QueryBuilderException
from src.query_builder.utils.escape import Escape


class Join(Escape):
    """Class to manage SQL joins."""

    _joins: List[str] = []

    def left_join(self, table: str, from_on: str, to_on: str, escape_on: bool = True, alias: Optional[str] = None) -> Self:
        """Add a LEFT JOIN clause."""
        return self._join(JoinDirection.LEFT.value, table, from_on, to_on, escape_on, alias)

    def right_join(self, table: str, from_on: str, to_on: str, escape_on: bool = True, alias: Optional[str] = None) -> Self:
        """Add a RIGHT JOIN clause."""
        return self._join(JoinDirection.RIGHT.value, table, from_on, to_on, escape_on, alias)

    def inner_join(self, table: str, from_on: str, to_on: str, escape_on: bool = True, alias: Optional[str] = None) -> Self:
        """Add an INNER JOIN clause."""
        return self._join(JoinDirection.INNER.value, table, from_on, to_on, escape_on, alias)

    def full_join(self, table: str, from_on: str, to_on: str, escape_on: bool = True, alias: Optional[str] = None) -> Self:
        """Add a FULL JOIN clause."""
        return self._join(JoinDirection.FULL.value, table, from_on, to_on, escape_on, alias)

    def _join(self, join_type: str, table: str, from_on: str, to_on: str, escape_on: bool = True, alias: Optional[str] = None) -> Self:
        """
        Add a join clause with the specified type.

        :param join_type: Type of the join (LEFT, RIGHT, INNER, FULL).
        :param table: The name of the table to join.
        :param from_on: The column from the base table.
        :param to_on: The column from the joining table.
        :param escape_on: Whether to escape the ON fields.
        :param alias: Optional alias for the joining table.
        :raises QueryBuilderException: If the table name is invalid.
        :return: self, for chaining purposes.
        """
        if not table.strip():
            raise QueryBuilderException("Table is required")

        table = self._key_escape(table)
        if escape_on:
            from_on = self._key_escape(from_on)
            to_on = self._key_escape(to_on)

        # Construct the join clause string
        join_clause = f"{join_type} JOIN {table} ON {from_on} = {to_on}"
        if alias is not None:
            join_clause = f"{join_type} JOIN {table} AS {alias} ON {from_on} = {to_on}"

        self._joins.append(join_clause)
        return self