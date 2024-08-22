import discord
from discord.ext import commands
from model import Counter

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import os

TOKEN = os.environ.get("TOKEN", "")

intents = discord.Intents.all()


class Bot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="!",
            intents=intents,
        )

    async def on_ready(self):
        print("起動")


bot = Bot()


@bot.group("db", invoke_without_command=True)
async def db(ctx: commands.Context): ...


@db.command()
async def create(ctx: commands.Context):
    db = Counter()
    await db.create_table(True)

    await ctx.send("テーブルを作成しました")


@bot.command("count")
async def _count(ctx: commands.Context):
    db = Counter()
    m = await ctx.send("1")
    db.message_id.set_value(m.id)
    db.count.set_value(1)
    await db.insert()


@bot.command("update")
async def _update(ctx: commands.Context, message_id: str):
    db = Counter()
    db.message_id.set_value(int(message_id))
    await db.fetch()

    if db.is_none:
        await ctx.send("データが見つかりませんでした。")
        return

    message = await ctx.fetch_message(int(message_id))

    db.count.set_value(db.count.value + 1)

    await message.edit(content=str(db.count.value))

    await db.update()


bot.run(TOKEN)
