from typing import List, Optional, Union, Self

from ..capabilities.from_capability import From
from ..capabilities.group import Group
from ..capabilities.join import Join
from ..capabilities.limit import Limit
from ..capabilities.order import Order
from ..capabilities.where import Where
from ..core.builder import Builder
from ..core.e_query import EQuery
from ..core.query import Query
from ..exceptions.query_builder_exception import QueryBuilderException


class Select(Where, From, Limit, Join, Group, Order):
    def __init__(self, factory=None):
        Where.__init__(self)
        From.__init__(self)
        Limit.__init__(self)
        Join.__init__(self)
        Group.__init__(self)
        Order.__init__(self)

        self._statements: List = []
        self._is_distinct: bool = False
        self._factory = factory

    def add_column(self, column: str, escape: bool = True) -> Self:
        """Add a single column to the select statement."""
        if self._statements and self._statements[0] == "*":
            raise QueryBuilderException("All Columns Selected")

        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column_to_add = self._key_escape(column) if escape else column
        self._statements.append(column_to_add)
        return self

    def add_columns(self, columns: List[str], escape: bool = True) -> Self:
        """Add multiple columns to the select statement."""
        for column in columns:
            self.add_column(column, escape)
        return self

    def add_all_columns(self) -> Self:
        """Select all columns (*)."""
        if self._statements:
            raise QueryBuilderException("Some Columns Already Set")

        self._statements.append("*")
        return self

    def add_column_sum(self, column: str, alias: Optional[str] = None,
                      escape_key: bool = True, escape_alias: bool = True) -> Self:
        """Add a SUM aggregate function."""
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"SUM({column})", False)
        else:
            return self.add_column_as_alias(f"SUM({column})", alias, False, escape_alias)

    def add_column_count(self, column: str = "*", alias: Optional[str] = None,
                        escape_key: bool = True, escape_alias: bool = True) -> Self:
        """Add a COUNT aggregate function."""
        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"COUNT({column})", False)
        else:
            return self.add_column_as_alias(f"COUNT({column})", alias, False, escape_alias)

    def add_column_average(self, column: str, alias: Optional[str] = None,
                         escape_key: bool = True, escape_alias: bool = True) -> Self:
        """Add an AVG aggregate function."""
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"AVG({column})", False)
        else:
            return self.add_column_as_alias(f"AVG({column})", alias, False, escape_alias)

    def add_column_max(self, column: str, alias: Optional[str] = None,
                      escape_key: bool = True, escape_alias: bool = True) -> Self:
        """Add a MAX aggregate function."""
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"MAX({column})", False)
        else:
            return self.add_column_as_alias(f"MAX({column})", alias, False, escape_alias)

    def add_column_min(self, column: str, alias: Optional[str] = None,
                      escape_key: bool = True, escape_alias: bool = True) -> Self:
        """Add a MIN aggregate function."""
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"MIN({column})", False)
        else:
            return self.add_column_as_alias(f"MIN({column})", alias, False, escape_alias)

    def add_column_as_alias(self, column: str, alias: str, escape_key: bool = True,
                           escape_alias: bool = True) -> Self:
        """Add a column with an alias."""
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        alias = self._key_escape(alias) if escape_alias else alias
        self.add_column(f"{column} AS {alias}", False)
        return self

    def set_distinct(self, enable: bool = True) -> Self:
        """Set whether to select distinct values."""
        self._is_distinct = enable
        return self

    def compile(self) -> Union[Query, EQuery]:
        """Compile the select query."""
        if not self._from_table or not self._from_table.strip():
            raise QueryBuilderException("From is Required")

        if not self._statements:
            self.add_all_columns()

        base_query = Builder.select(self._statements, self._is_distinct)
        base_query += Builder.from_clause(self._from_table)
        base_query += Builder.joins(self._joins)
        base_query += Builder.where(self._where_statements)
        base_query += Builder.group_by(self._group_by)
        base_query += Builder.order_by(self._order_by)

        if self._count is not None:
            base_query += Builder.count(self._count)
            if self._offset is not None:
                base_query += Builder.offset(self._offset)

        return EQuery(base_query, self._factory) if self._factory else Query(base_query)