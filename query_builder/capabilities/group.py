from typing import List, Union, Self

from query_builder.exceptions.query_builder_exception import QueryBuilderException
from query_builder.utils.escape import Escape


class Group(Escape):
    """Class to manage GROUP BY clause for SQL queries."""

    _group_by: List[str] = []

    def group_by(self, group_columns: Union[str, List[str]], escape: bool = True) -> Self:
        """
        Set the GROUP BY clause for SQL operations.

        :param group_columns: A string or list of columns to group by.
        :param escape: Whether to escape the columns.
        :raises QueryBuilderException: If the columns are invalid.
        :return: self, for chaining purposes.
        """
        if isinstance(group_columns, str):
            if not group_columns.strip():
                raise QueryBuilderException("GroupBy can't set as empty string")

            if escape:
                group_columns = self._key_escape(group_columns)

            self._group_by.append(group_columns)

        elif isinstance(group_columns, list):
            if len(group_columns) == 0:
                raise QueryBuilderException("GroupBy can't set empty")

            for i, column in enumerate(group_columns):
                if not column.strip():
                    raise QueryBuilderException("GroupBy can't set as empty string")

                if escape:
                    group_columns[i] = self._key_escape(column)
            self._group_by.extend(group_columns)
        else:
            raise QueryBuilderException("group_columns must be a string or list of strings")

        return self
