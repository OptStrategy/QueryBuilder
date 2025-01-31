from typing import List, Optional, Union

from src.query_builder.capabilities.group import Group
from src.query_builder.capabilities.join import Join
from src.query_builder.capabilities.limit import Limit
from src.query_builder.capabilities.order import Order
from src.query_builder.capabilities.where import Where
from src.query_builder.capabilities.from_capability import From
from src.query_builder.core.builder import Builder
from src.query_builder.core.db_factory import DBFactory
from src.query_builder.exceptions.query_builder_exception import QueryBuilderException


class Select(Where, From, Limit, Join, Group, Order):
    _statements: List = []
    _id_distinct: bool = False

    def __init__(
            self,
            factory: Optional[DBFactory] = None
    ):
        self.factory = factory

    def add_column(self, column: str, escape: bool = True) -> 'Select':
        if self._statements and self._statements[0] == "*":
            raise QueryBuilderException("All Columns Selected")

        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column_to_add = self._key_escape(column) if escape else column
        self._statements.append(column_to_add)
        return self

    def add_columns(self, columns: List[str], escape: bool = True) -> 'Select':
        for column in columns:
            self.add_column(column, escape)
        return self

    def add_all_columns(self) -> 'Select':
        if self._statements:
            raise QueryBuilderException("Some Columns Already Set")

        self._statements.append("*")
        return self

    def add_column_sum(self, column: str, alias: Optional[str] = None,
                       escape_key: bool = True, escape_alias: bool = True) -> 'Select':
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"SUM({column})", False)
        else:
            return self.add_column_as_alias(f"SUM({column})", alias, False, escape_alias)

    def add_column_count(self, column: str = "*", alias: Optional[str] = None,
                         escape_key: bool = True, escape_alias: bool = True) -> 'Select':
        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"COUNT({column})", False)
        else:
            return self.add_column_as_alias(f"COUNT({column})", alias, False, escape_alias)

    def add_column_average(self, column: str, alias: Optional[str] = None,
                           escape_key: bool = True, escape_alias: bool = True) -> 'Select':
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"AVG({column})", False)
        else:
            return self.add_column_as_alias(f"AVG({column})", alias, False, escape_alias)

    def add_column_max(self, column: str, alias: Optional[str] = None,
                       escape_key: bool = True, escape_alias: bool = True) -> 'Select':
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"MAX({column})", False)
        else:
            return self.add_column_as_alias(f"MAX({column})", alias, False, escape_alias)

    def add_column_min(self, column: str, alias: Optional[str] = None,
                       escape_key: bool = True, escape_alias: bool = True) -> 'Select':
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        if alias is None:
            return self.add_column(f"MIN({column})", False)
        else:
            return self.add_column_as_alias(f"MIN({column})", alias, False, escape_alias)

    def add_column_as_alias(self, column: str, alias: str, escape_key: bool = True,
                            escape_alias: bool = True) -> 'Select':
        if not column:
            raise QueryBuilderException("Column cannot be empty")

        column = self._key_escape(column) if escape_key else column
        alias = self._key_escape(alias) if escape_alias else alias
        self.add_column(f"{column} AS {alias}", False)
        return self

    def set_distinct(self, enable: bool = True) -> 'Select':
        self.is_distinct = enable
        return self

    def compile(self) -> Union['Query', 'EQuery']:
        if not self.from_table or not self.from_table.strip():
            raise QueryBuilderException("From is Required")

        if not self._statements:
            self.add_all_columns()

        base_query = Builder.select(self._statements, self.is_distinct)
        base_query += Builder.from_clause(self._from_table)
        base_query += Builder.joins(self._joins)
        base_query += Builder.where(self._where_statements)
        base_query += Builder.group_by(self._group_by)
        base_query += Builder.order_by(self._order_by)

        if self._count is not None:
            base_query += Builder.count(self._count)
            if self._offset is not None:
                base_query += Builder.offset(self._offset)

        return EQuery(base_query, self.factory) if self.factory else Query(base_query)