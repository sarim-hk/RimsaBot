import discord
from discord import app_commands
from discord.ext import commands

class General(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="pingy", description="Check bot's latency")
    async def ping(self, interaction: discord.Interaction):
        assert self.bot.user is not None
        await interaction.response.send_message(
            f"Pong! {self.bot.user.name}'s latency is {round(self.bot.latency * 1000)}ms."
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(General(bot))
