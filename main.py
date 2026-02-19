import discord
import logging
import json
from discord.ext import commands

class RimsaBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(
            command_prefix="!",
            intents=intents
        )

    async def setup_hook(self):
        await self.load_extension("cogs.general")
        await self.tree.sync()

    async def on_ready(self):
        print(f"Logged on as {self.user}!")

if __name__ == "__main__":
    with open("config.json", "r") as f:
        _cfg = json.load(f)

    handler = logging.FileHandler(
        filename="discord.log",
        encoding="utf-8",
        mode="w"
    )

    bot = RimsaBot()
    bot.run(
        _cfg.get("DISCORD_TOKEN"),
        log_handler=handler,
        log_level=logging.DEBUG
    )
