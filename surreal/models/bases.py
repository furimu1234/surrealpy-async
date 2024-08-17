from __future__ import annotations

from typing import Any, Self

from surreal import (
    BaseTable,
    Column,
    Query,
    log_delete,
    log_insert,
    log_res,
    log_select,
    log_update,
)
from surreal._types import Array, Object, String

EMBED_DEFAULT_COLOR = "#05d0f3"

__all__ = ("EmbedContentPanelTable",)


class EmbedContentPanelTable(BaseTable):
    content: Column[str | None] = Column(name="content", type=String(is_none=True))
    title: Column[str | None] = Column(name="title", type=String(is_none=True))
    description: Column[str | None] = Column(
        name="description", type=String(is_none=True)
    )
    color: Column[str | None] = Column(
        name="color", type=String(), default=EMBED_DEFAULT_COLOR
    )
    fields: Column[list[dict[str, Any]]] = Column(
        name="fields", type=Array(Object()), default=[]
    )

    async def create_table(self) -> Self:
        q = Query()
        q.original(self.set_schemafull())
        q.define_field(self, self.content)
        q.define_field(self, self.title)
        q.define_field(self, self.description)
        q.define_field(self, self.color)
        q.define_field(self, self.fields)
        res = (await self.executes(q.to_string()))["result"]
        log_res(res)
        return self

    def set_data(self, res: Any) -> Self:
        if not isinstance(res, dict):
            self.is_none = True
            return self

        self.id = res.get("id", 0)
        self.set_table_name()
        self.content.set_value(res.get("content", ""))
        self.title.set_value(res.get("title", ""))
        self.description.set_value(res.get("description", ""))
        self.color.set_value(res.get("color", ""))
        self.fields.set_value(res.get("fields", []))
        self.is_none = False
        return self

    async def fetch(self) -> Self:
        q = Query()
        q.select(self)

        log_select(q)

        response = (await self.executes(q.to_string()))["result"]
        if not response:
            self.is_none = True
            return self
        res = response[0]
        log_res(res)

        return self.set_data(res)

    async def insert(self) -> Self:
        q = Query()
        q.insert(self)
        q.add_sqlvalue(self.content)
        q.add_sqlvalue(self.title)
        q.add_sqlvalue(self.description)
        q.add_sqlvalue(self.color)
        q.add_sqlvalue(self.fields)

        log_insert(q)
        res = (await self.executes(q.to_string()))["result"][0]
        log_res(res)

        return self.set_data(res)

    async def update(self) -> Self:
        q = Query()
        q.update(self)
        q.add_sqlvalue(self.content)
        q.add_sqlvalue(self.title)
        q.add_sqlvalue(self.description)
        q.add_sqlvalue(self.color)
        q.add_sqlvalue(self.fields)

        log_update(q)
        res = (await self.executes(q.to_string()))["result"][0]
        log_res(res)

        return self.set_data(res)

    async def delete(self) -> bool:
        q = Query()
        q.delete(self)

        log_delete(q)

        res = (await self.executes(q.to_string()))["result"]
        log_res(res)

        if not isinstance(res, list):
            return False
        return True
