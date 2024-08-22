from __future__ import annotations

from typing import Any, Self

from surreal._types import Int
from surreal.column import Column
from surreal.query import Query
from surreal.table import BaseTable


class Counter(BaseTable):
    message_id: Column[int] = Column(name="message_id", type=Int())
    count: Column[int] = Column(name="count", type=Int())

    def set_data(self, res: Any) -> Self:
        self.is_none = False

        if not isinstance(res, dict):
            self.is_none = True
            return self

        self.message_id.set_value(res.get(self.message_id.name, 0))
        self.count.set_value(res.get(self.count.name, 0))

        return self

    async def create_table(self, execute: bool = False) -> Query:
        q = Query()
        q.define_field(self, self.message_id)
        q.define_field(self, self.count)

        if execute:
            await self.executes(q.to_string())

        return q

    async def fetch(self) -> Self:
        q = Query()
        q.select(self, True)
        q.where(f"{self.message_id.name} = {self.message_id}")

        response = (await self.execute(q.to_string()))["result"]

        if not isinstance(response, list):
            raise Exception(response)

        return self.set_data(response[0])

    async def insert(self) -> Self:
        q = Query()
        q.insert(self)
        q.add_sqlvalue(self.message_id)
        q.add_sqlvalue(self.count)

        response = (await self.executes(q.to_string()))["result"]

        if not isinstance(response, list):
            raise Exception(response)

        return self.set_data(response[0])

    async def update(self) -> Self:
        q = Query()
        q.update(self)
        q.add_sqlvalue(self.message_id)
        q.add_sqlvalue(self.count)

        response = (await self.executes(q.to_string()))["result"]

        if not isinstance(response, list):
            raise Exception(response)

        return self.set_data(response[0])

    async def delete(self) -> Self:
        q = Query()
        q.delete(self)

        response = (await self.execute(q.to_string()))["result"]

        if not isinstance(response, list):
            raise Exception(response)

        return self.set_data(response[0])
