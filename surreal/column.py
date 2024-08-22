from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Generic, Self, TypeVar

from pydantic import BaseModel, PlainValidator

from ._types import DBType
from .utils import validate

__all__ = ("Column",)

T = TypeVar("T")


class Column(BaseModel, Generic[T]):
    name: str | None = None
    type: Annotated[DBType, PlainValidator(validate)]  # OR未対応
    value: Any = None
    datetime_format: str = "%Y/%m/%dT%H:%M:%SZ"
    default: T | None = None

    def __str__(self):
        return str(self.get_value())

    def set_value(self, new_value: T) -> Self:
        """valueに値を設定する。型ヒントが欲しい時用

        Parameters
        ----------
        new_value : T
            設定する値

        Returns
        -------
        Self
            インスタンス
        """
        self.value = new_value
        return self

    def get_value(self, is_datetime_to_str: bool = False) -> T:
        """値を取得する。型ヒントが欲しい時用

        Returns
        -------
        T
            値
        """

        if is_datetime_to_str and isinstance(self.value, datetime):
            return self.value.strftime(self.datetime_format)  # type: ignore

        return self.value

    def append_value(self, new_value: T) -> list[T]:
        """値をリストに追加する。

        Parameters
        ----------
        new_value : Any
            追加する値

        Returns
        -------
        list[T]
        """

        if not isinstance(self.value, list):
            self.value = [new_value]
        else:
            self.value.append(new_value)
        return self.value

    def remove_value(self, new_value: T) -> list[T]:
        """値をリストから削除する。

        Parameters
        ----------
        new_value : T
            削除する値

        Returns
        -------
        list[T]
        """

        if not isinstance(self.value, list):
            self.value = []

        else:
            self.value.remove(new_value)
        return self.value
