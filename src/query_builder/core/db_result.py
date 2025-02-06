from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class DBResult:
    is_success: bool
    rows: Optional[List[Dict[str, Any]]] = None
    count: Optional[int] = 0
    insert_id: Optional[int] = None
    affected_rows: Optional[int] = None
    message: Optional[str] = None  # In case of error it represents the error_message, else it may represent the executed query
