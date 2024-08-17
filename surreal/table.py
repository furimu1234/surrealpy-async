from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Self

import aiohttp
from pydantic import BaseModel, Field, model_validator

from ._types import ManyResultResponseType, OneResultResponseType
from .column import Column
from .utils import log

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


__all__ = ("BaseTable",)

DB = os.environ.get("SurrealDB_HOST")
USER = os.environ.get("SurrealDB_USER")
PASSWORD = os.environ.get("SurrealDB_PASSWORD")


assert isinstance(DB, str)
assert isinstance(USER, str)
assert isinstance(PASSWORD, str)


class BaseTable(BaseModel):
    table_name: str = Field(default="", exclude=True)
    id: str | int | None = Field(default=None, exclude=True)
    ns: str = Field(default="same", exclude=True)
    db: str = Field(default="same", exclude=True)
    host: str = Field(default=DB, exclude=True)
    user: str = Field(default=USER, exclude=True)
    password: str = Field(default=PASSWORD, exclude=True)
    is_none: bool = Field(default=True, exclude=True)
    result_time: str = Field(default="", exclude=True)


    def str_to_datetime(self, res: Any, key: str | None = "") -> datetime:
        """文字列からdatetime型に変換する。

        Parameters
        ----------
        res : Any
            レスポンスデータ
        key : str | None, optional
            変換したい値のキー, by default ""

        Returns
        -------
        datetime
            変換後のデータ
        """
        if not isinstance(res.get(key), datetime):
            datetime_to_str = datetime.strptime(
                res.get(key, "1970-1-1T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ"
            )
        else:
            datetime_to_str: datetime = res[key]  # type: ignore

        return datetime_to_str

    def get_id(self) -> str:
        """コロン以降のIDを取得する。

        Returns
        -------
        str
            ID
        """
        if self.id is None:
            return ""

        if ":" not in str(self.id):
            return str(self.id)

        return list(str(self.id).split(":"))[1]

    def set_default(self):
        """デフォルト値をプロパティに設定する。
        """

        datas = self.model_dump()

        for k, v in datas.items():
            if not isinstance(v, dict):
                setattr(self, k, v)
                continue

            if v.get("default") is not None:
                setattr(
                    self,
                    k,
                    Column(
                        name=v["name"],
                        type=v["type"],
                        value=v.get("default"),
                        default=v.get("default"),
                        datetime_format=v.get("datetime_format", "%Y/%m/%dT%H:%M:%SZ"),
                    ),
                )

    def set_schemafull(self) -> str:
        """スキーマフルに設定する。

        Returns
        -------
        str
            sql
        """

        return f"DEFINE TABLE {self.table_name} SCHEMAFULL;"

    def set_table_name(self) -> "BaseTable":
        """テーブル名を設定する。

        Returns
        -------
        BaseTable
            インスタンス
        """

        self.table_name = self.__class__.__qualname__.lower()

        if self.id is not None:
            if ":" in str(self.id):
                self.table_name = str(self.id)
            else:
                self.table_name += ":" + str(self.id)

        return self

    @model_validator(mode="after")
    def create_table_name(self) -> Self:
        """インスタンス化した後にテーブル名の値を変更する

        クラス名を小文字にし、idに値があれば、:とidの値をtable_nameに追加する

        Returns
        -------
        Self
            _description_
        """

        return self.set_table_name()

    async def __request(self, sql: str, headers: dict[str, str]) -> list[dict]:
        """sqlを実行する。

        Parameters
        ----------
        sql : str
            実行するsql
        headers : dict[str, str]
            ヘッダー

        Returns
        -------
        レスポンス
        """

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(login=self.user, password=self.password)
        ) as session:
            async with session.post(
                self.host + "/sql",
                data=sql,
                headers=headers,
            ) as response:
                return await response.json()

    async def executes(self, sql: str) -> ManyResultResponseType:
        """sqlを実行する

        Parameters
        ----------
        sql : str
            任意のsql

        Returns
        -------
        ManyResultResponseType
            resultがリスト

        Raises
        ------
        Exception
            エラー
        """
        headers = {
            "Accept": "application/json",
            "ns": self.ns,
            "db": self.db,
            "content-type": "application/json",
        }

        for _ in range(5):
            try:
                response_data = await self.__request(sql, headers)
                break
            except aiohttp.ContentTypeError:
                continue
        else:
            raise Exception("ContentTypeError")

        if isinstance(response_data, dict):
            code = response_data.get("code", 0)
            details = response_data.get("details", "UnknownDetails")
            information = response_data.get("information", "UnknownInformation")
            return [code, details, information]


        else:
            if isinstance(response_data, list) and isinstance(
                response_data[0]["result"], dict
            ):
                log.warning(response_data)

            if isinstance(response_data[0]["result"], list):
                result_length = len(response_data[0]["result"])
                if 1 <= result_length:
                    if response_data[0].get("time"):
                        self.result_time = response_data[0].get("time", "")

            return response_data[0]

    async def execute(self, sql: str) -> OneResultResponseType:
        """sqlを実行する

        Parameters
        ----------
        sql : str
            任意のsql

        Returns
        -------
        OneResultResponseType
            resultに1つだけ値が入ってる
        """

        response = await self.executes(sql)
        return {
            "code": response.get("code", ""),
            "result": response.get("result", ""),  # type: ignore
            "description": response.get("description", ""),
            "time": response["time"],
        }

