class Builder:
    @staticmethod
    def as_alias(alias_name: str) -> str:
        return f" AS {alias_name}"

    @staticmethod
    def set_on_duplicate_key_update(updates: dict) -> str:
        base_query = " ON DUPLICATE KEY UPDATE "
        base_query += ", ".join(f"{key} = {value}" for key, value in updates.items())
        return base_query

    @staticmethod
    def set_delete_table(from_table: str) -> str:
        return f"DELETE FROM {from_table}"

    @staticmethod
    def set_insert_rows(rows: list) -> str:
        return ", ".join(f"({', '.join(map(str, row))})" for row in rows)

    @staticmethod
    def set_insert_columns(columns: list) -> str:
        return f"({', '.join(columns)}) VALUES "

    @staticmethod
    def set_insert_table(into_table: str) -> str:
        return f"INSERT INTO {into_table} "

    @staticmethod
    def set_updates(updates: list) -> str:
        return ", ".join(updates) if updates else ""

    @staticmethod
    def set_update_table(table: str) -> str:
        return f"UPDATE {table} SET "

    @staticmethod
    def select(statements: list, is_distinct: bool) -> str:
        base_query = "SELECT "
        if is_distinct:
            base_query += "DISTINCT "
        base_query += ", ".join(statements)
        return base_query

    @staticmethod
    def from_clause(from_table: str) -> str:
        return f" FROM {from_table} "

    @staticmethod
    def joins(joins: list) -> str:
        return " ".join(joins) if joins else ""

    @staticmethod
    def where(where_statements: list) -> str:
        if where_statements:
            conditions = " OR ".join(
                f"({' AND '.join(statement)})" for statement in where_statements if statement
            )
            return f" WHERE {conditions}"
        return ""

    @staticmethod
    def group_by(group_by: list) -> str:
        return f" GROUP BY {', '.join(group_by)}" if group_by else ""

    @staticmethod
    def order_by(order_by: dict) -> str:
        if not order_by:
            return ""
        base_query = " ORDER BY"
        ordered = ", ".join(
            f"{column} {direction}".strip() for column, direction in order_by.items()
        )
        return f"{base_query} {ordered}"

    @staticmethod
    def offset(offset: int) -> str:
        return f" OFFSET {offset}" if offset else ""

    @staticmethod
    def count(count: int) -> str:
        return f" LIMIT {count}" if count else ""
