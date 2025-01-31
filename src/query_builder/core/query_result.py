from typing import Optional, List


class QueryResult:
    def __init__(
            self,
            insert_id: Optional[int] = None,
            affected_rows: Optional[int] = None,
            result_fields: Optional[List[str]] = None,
            result_rows: Optional[List[dict]] = None,
            warning_count: Optional[int] = None
    ):
        self.insert_id = insert_id
        self.affected_rows = affected_rows
        self.result_fields = result_fields
        self.result_rows = result_rows
        self.warning_count = warning_count

    def __repr__(self) -> str:
        return f"QueryResult(insert_id={self.insert_id}, affected_rows={self.affected_rows}, " \
               f"result_fields={self.result_fields}, result_rows={self.result_rows}, warning_count={self.warning_count})"
