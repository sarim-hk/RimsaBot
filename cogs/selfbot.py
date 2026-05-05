
import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot

class SelfBot(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.cfg = bot.cfg

    @app_commands.command(name="restream", description="Restream a Discord stream.")
    async def restream(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)

        selfbot_user = await self.bot.fetch_user(int(self.cfg["SELFBOT_USER_ID"]))
        await selfbot_user.send("hello world")

        await interaction.followup.send("Sent `hello world` to the selfbot user.")

async def setup(bot: RimsaBot):
    await bot.add_cog(SelfBot(bot))

