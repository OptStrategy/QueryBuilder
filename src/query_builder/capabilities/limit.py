from typing import Self, Union

from src.query_builder.exceptions.query_builder_exception import QueryBuilderException
from src.query_builder.utils.escape import Escape


class Limit(Escape):
    """Class to manage limit and offset in SQL queries."""

    _offset: int
    _count: int

    def set_offset(self, offset: Union[int, float]) -> Self:
        """
        Set the offset for the SQL query.

        :param offset: The offset value to be set.
        :raises QueryBuilderException: If the offset is already set.
        :return: self, for chaining purposes.
        """
        if self._offset is not None:
            raise QueryBuilderException("Offset already set")

        self._offset = offset
        return self

    def set_limit(self, count: Union[int, float]) -> Self:
        """
        Set the limit for the SQL query.

        :param count: The count/limit value to be set.
        :raises QueryBuilderException: If the limit is already set.
        :return: self, for chaining purposes.
        """
        if self._count is not None:
            raise QueryBuilderException("Count already set")

        self._count = count
        return self
