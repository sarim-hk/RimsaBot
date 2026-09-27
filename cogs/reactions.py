import discord
from discord.ext import commands
from bot import RimsaBot
from utils.database import Database


class Reactions(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.database = Database(bot.cfg)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        self.database.insert_reaction(payload)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        self.database.remove_reaction(payload)

async def setup(bot: RimsaBot):
    await bot.add_cog(Reactions(bot))
