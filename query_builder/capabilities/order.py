from typing import Union, Dict, Self

from query_builder.enums.order_direction import OrderDirection
from query_builder.exceptions.query_builder_exception import QueryBuilderException
from query_builder.utils.escape import Escape


class Order(Escape):
    _order_by: Dict = {}

    def add_order(self, order_column: str, order_direction: Union[OrderDirection, str] = OrderDirection.NONE,
                  escape: bool = True) -> Self:
        """
        Add an order-by clause to the SQL query.

        :param order_column: The column to order by.
        :param order_direction: The direction of ordering (ASC, DESC, NONE).
        :param escape: Whether to escape the column name. Defaults to True.
        :raises QueryBuilderException: If the order column is empty or direction is invalid.
        :return: self, for chaining purposes.
        """
        if not order_column.strip():
            raise QueryBuilderException("OrderColumn can't be set as an empty string")

        if isinstance(order_direction, str):
            try:
                order_direction = OrderDirection(order_direction)
            except ValueError:
                raise QueryBuilderException("OrderDirection Not Valid")

        if escape:
            order_column = self._key_escape(order_column)

        self._order_by[order_column] = order_direction.value
        return self

    def add_random_order(self) -> Self:
        """
        Add a random ordering to the SQL query.

        :return: self, for chaining purposes.
        """
        self._order_by['RAND()'] = ''
        return self