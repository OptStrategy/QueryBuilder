class Query:
    def __init__(self, query: str):
        self._query = query

    def get_query_as_string(self) -> str:
        """Return the query as a string."""
        return self._query

    def get_query(self) -> str:
        """Return the query. This method calls get_query_as_string for compatibility."""
        return self.get_query_as_string()