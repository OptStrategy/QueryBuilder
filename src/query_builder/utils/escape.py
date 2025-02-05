from ..exceptions.query_builder_exception import QueryBuilderException


class Escape:
    def _key_escape(self, value: str) -> str:
        """Escape a string to safely use it as a SQL key."""
        if not value.strip():
            raise QueryBuilderException("Cannot escape empty string")

        parts = value.split(".")
        escaped_str = ""
        for part in parts:
            if part != '*':
                escaped_str += f"`{part}`."
            else:
                escaped_str += f"{part}."

        return escaped_str.rstrip(".")

    def _escape(self, value):
        """Escape a value for safe insertion into a SQL query."""
        if value is None:
            return 'NULL'

        if isinstance(value, (int, float)):
            return value

        if isinstance(value, bool):
            return int(value)

        return f"'{self._escape_string(str(value))}'"

    def _escape_string(self, query: str) -> str:
        """Escape special characters in a string for SQL use."""
        replacement_map = {
            "\0": "\\0",
            "\n": "\\n",
            "\r": "\\r",
            "\t": "\\t",
            chr(26): "\\Z",
            chr(8): "\\b",
            '"': '\\"',
            "'": "\\'",
            # '_': "\\_",
            # '%': "\\%",
            '\\': '\\\\'
        }

        # Replace characters in the query using the replacement map
        for char, replacement in replacement_map.items():
            query = query.replace(char, replacement)

        return query
