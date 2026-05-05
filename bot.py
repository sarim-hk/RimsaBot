import discord
from discord.ext import commands

class RimsaBot(commands.Bot):
    def __init__(self, cfg: dict[str, str]):
        self.cfg: dict[str, str] = cfg
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(
            command_prefix="!",
            intents=intents
        )

    async def setup_hook(self):
        await self.load_extension("cogs.general")
        await self.load_extension("cogs.dathost")
        await self.load_extension("cogs.selfbot")
        await self.tree.sync()

    async def on_ready(self):
        print(f"Logged on as {self.user}!")
