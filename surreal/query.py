from __future__ import annotations

import copy
import json
from datetime import datetime
from typing import TYPE_CHECKING

from ._types import Array, Bool, Datetime, Object, Record, String,Bytes
from .table import BaseTable
from .utils import MISSING

if TYPE_CHECKING:
    from .column import Column

__all__ = ("Query",)


class Query:
    def __init__(self):
        self.__query = ""
        self.q = ""

    def add_quotation(self, v: str | None = None):
        return f"'{v}'"

    def list_join(self, value: list) -> str:
        sql = "["
        if not value:
            return sql + "]"

        if isinstance(value[0], datetime) and isinstance(value[1], str):
            datetime_value = value[0].strftime(value[1])
            sql = f"'{datetime_value}',"
            return sql

        for v in value:
            if isinstance(v, str):
                sql += f"{self.add_quotation(v)},"

            elif isinstance(v, list):
                if not v:
                    continue

                elif isinstance(v[0], datetime) and isinstance(v[1], str):
                    datetime_value = v[0].strftime(v[1])
                    sql += f"'{datetime_value}',"
                    continue

                elif isinstance(v[0], list):
                    sql += self.list_join(v[0]) + ","
                    continue

                sql += self.list_join(v) + ","

            elif isinstance(v, BaseTable):
                sql += f"{v.table_name},"

            elif isinstance(v, dict):
                sql += json.dumps(v) + ","

            else:
                sql += f"{v},"

        return sql + "]"

    def schemafull(self, table: BaseTable) -> None:
        self.__query += f"DEFINE TABLE {table.table_name} SCHEMAFULL;\n"

    def remove_field(self, table: BaseTable, col: Column) -> None:
        self.__query += (
            f"REMOVE FIELD IF EXISTS {col.name} ON TABLE {table.table_name};"
        )

    def add_field(self, table: BaseTable, col: Column) -> None:
        self.define_field(table, col)

    def add_string(self, col: Column) -> None:
        if col.value is None:
            self.__query += f"{col.name} = None,"
            return
        elif col.value == "":
            self.__query += f"{col.name} = '',"
            return

        self.__query += f"{col.name} = '{col.value}',"

    def add_normal(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return

        self.__query += f"{col.name} = {col.value},"
    
    def add_byte(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return
        value = col.value
        self.__query += f'{col.name} = <bytes>"{value}",'

    def add_object(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return
        self.__query += f"{col.name} = {json.dumps(col.value)},"

    def add_bool(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return
        self.__query += f"{col.name} = {col.value},"

    def add_record(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return

        if isinstance(col.value, str):
            self.__query += f"{col.name} = {col.value},"
        else:
            self.__query += f"{col.name} = {col.value.table_name},"

    def add_array(self, col: Column) -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = [],"
            return

        self.__query += str(col.name) + "=" + self.list_join(col.value) + ","

    def add_datetime(self, col: Column, _format: str = "%Y-%m-%dT%H:%M:%SZ") -> None:
        if col.value is None or col.value == "":
            self.__query += f"{col.name} = None,"
            return

        value = col.value.strftime(_format)
        self.__query += f"{col.name} = return type::datetime('{value}'),"

    def add_sqlvalue(self, col: Column, _format: str = "%Y-%m-%dT%H:%M:%SZ") -> None:
        _type = type(col.type)
        if _type is Array:
            self.add_array(col)
        elif _type is String:
            self.add_string(col)
        elif _type is Bool:
            self.add_bool(col)
        elif _type is Record:
            self.add_record(col)
        elif _type is Object:
            self.add_object(col)
        elif _type is Datetime:
            self.add_datetime(col, _format)
        elif _type is Bytes:
            self.add_byte(col)
        else:
            self.add_normal(col)

    def select(self, table: BaseTable, ignore_id: bool = False) -> None:
        if ignore_id:
            table_name = list(table.table_name.split(":"))[0]
        else:
            table_name = table.table_name

        self.__query += f"SELECT * FROM {table_name}"

    def where(self, where: str) -> None:
        self.__query += f" WHERE {where} "

    def fetch(self, fetch: str) -> None:
        self.__query += f" FETCH {fetch} "

    def insert(self, table: BaseTable) -> None:
        self.__query += f"CREATE {table.table_name} SET "

    def update(self, table: BaseTable) -> None:
        self.__query += f"UPDATE {table.table_name} SET "

    def delete(self, table: BaseTable) -> None:
        self.__query += f"DELETE FROM {table.table_name} "

    def limit(self, limit: int) -> None:
        self.__query += f"LIMIT {limit}"

    def asc(self, column: Column) -> None:
        self.__query += f"ORDER BY {column.name} ASC "

    def desc(self, column: Column) -> None:
        self.__query += f"ORDER BY {column.name} DESC "

    def original(self, original_sql: str) -> None:
        self.q += original_sql

    def define_field(self, table: BaseTable, col: Column) -> None:
        if ":" in table.table_name:
            table_name = list(table.table_name.split(":"))[0]
        else:
            table_name = table.table_name

        _type = col.type
        __type = col.type

        while __type is not MISSING:
            if str(_type).endswith(">"):
                parent_type = (
                    str(copy.deepcopy(_type)).replace(">", "") + "<{sub_type}>>"
                )
            else:
                parent_type = str(copy.deepcopy(_type)) + "<{sub_type}>"
            try:
                _sub_type = list(str(__type.sub_type).split("."))[-1]
                sub_type = list(str(_sub_type).split("'"))[0]

                _type = f"{parent_type}".format(sub_type=sub_type)
            except AttributeError:
                break
            __type = __type.sub_type

        define_query = f"DEFINE FIELD {col.name} ON TABLE {table_name} TYPE {str(_type).lower().replace('<>', '')} "

        if col.default is not None and col.default != "":
            if isinstance(col.type, String):
                value = f"'{col.default}'"
            elif isinstance(col.type, Datetime):
                _value = col.default.strftime("%Y-%m-%dT%H:%M:%SZ")
                value = f"'{_value}'"
            else:
                value = col.default

            define_query += f"DEFAULT {value}"

        self.__query += define_query + ";"

    def to_string(self) -> str:
        query = self.__query

        if not query.endswith(";"):
            query += ";"

        return (
            query
            # .replace(", ", " ")
            .replace(",,", ",")
            .replace(",]", "]")
            .replace(",;", ";")
            .replace(";\n", ";")
        )
