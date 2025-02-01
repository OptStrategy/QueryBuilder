from typing import List, Self

from ..utils.escape import Escape


class Where(Escape):
    _where_statements: List = []

    def where(self, key: str, value, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with an equality condition."""
        if escape_value:
            value = self._escape(value)

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "=", value)
        return self

    def where_not_equal(self, key: str, value, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'not equal' condition."""
        if escape_value:
            value = self._escape(value)

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "!=", value)
        return self

    def where_in(self, key: str, values, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with an 'IN' condition."""
        if escape_value:
            values = [self._escape(v) for v in values]

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "IN", f"({', '.join(map(str, values))})")
        return self

    def where_query(self, query: str) -> Self:
        """Add a custom WHERE clause (non-escaped)."""
        self._where_statements.append([query])
        return self

    def where_not_in(self, key: str, values, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'NOT IN' condition."""
        if escape_value:
            values = [self._escape(v) for v in values]

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "NOT IN", f"({', '.join(map(str, values))})")
        return self

    def where_greater(self, key: str, greater_than, greater_equals=False, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'greater than' condition."""
        if escape_value:
            greater_than = self._escape(greater_than)

        if escape_key:
            key = self._key_escape(key)

        operator = ">=" if greater_equals else ">"
        self.append(key, operator, greater_than)
        return self

    def where_lesser(self, key: str, lesser_than, lesser_equals=False, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'less than' condition."""
        if escape_value:
            lesser_than = self._escape(lesser_than)

        if escape_key:
            key = self._key_escape(key)

        operator = "<=" if lesser_equals else "<"
        self.append(key, operator, lesser_than)
        return self

    def where_between(self, key: str, less_value, great_value, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'BETWEEN' condition."""
        if escape_value:
            less_value = self._escape(less_value)
            great_value = self._escape(great_value)

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "BETWEEN", f"{less_value} AND {great_value}")
        return self

    def where_not_between(self, key: str, less_value, great_value, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'NOT BETWEEN' condition."""
        if escape_value:
            less_value = self._escape(less_value)
            great_value = self._escape(great_value)

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "NOT BETWEEN", f"{less_value} AND {great_value}")
        return self

    def where_is_null(self, key: str, escape_key=True) -> Self:
        """Add a WHERE clause with an 'IS NULL' condition."""
        if escape_key:
            key = self._key_escape(key)

        self.append(key, "IS", "NULL")
        return self

    def where_is_not_null(self, key: str, escape_key=True) -> Self:
        """Add a WHERE clause with an 'IS NOT NULL' condition."""
        if escape_key:
            key = self._key_escape(key)

        self.append(key, "IS NOT", "NULL")
        return self

    def where_like(self, key: str, value, begin=True, end=True, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'LIKE' condition."""
        if escape_value:
            if begin:
                value = f"%{value}"
            if end:
                value = f"{value}%"
            value = self._escape(value)
        else:
            if begin:
                value = f"'%' {value}"
            if end:
                value = f"{value} '%'"

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "LIKE", value)
        return self

    def where_not_like(self, key: str, value, begin=True, end=True, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause with a 'NOT LIKE' condition."""
        if escape_value:
            if begin:
                value = f"%{value}"
            if end:
                value = f"{value}%"
            value = self._escape(value)
        else:
            if begin:
                value = f"'%' {value}"
            if end:
                value = f"{value} '%'"

        if escape_key:
            key = self._key_escape(key)

        self.append(key, "NOT LIKE", value)
        return self

    def where_group(self, key_values, escape_value=True, escape_key=True) -> Self:
        """Add a WHERE clause for a group of conditions."""
        for key, value in key_values.items():
            if isinstance(value, bool):
                self.where(key, value, escape_value=True, escape_key=escape_key)
            else:
                self.where(key, value, escape_value, escape_key)

        return self

    def or_condition(self) -> Self:
        """Start a new OR condition group."""
        self._where_statements.append([])
        return self

    def append(self, key, operator, value):
        """Append a condition to the WHERE statement."""
        if len(self._where_statements) == 0:
            state = 0
        else:
            state = len(self._where_statements) - 1

        self._where_statements[state].append(f"{key} {operator} {value}")
