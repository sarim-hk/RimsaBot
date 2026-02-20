import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot

class General(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot

    @app_commands.command(name="ping")
    async def ping(self, interaction: discord.Interaction):
        if not self.bot.user:
            raise RuntimeError("Bot user doesn't exist!")
        
        await interaction.response.send_message(
            f"Pong! {self.bot.user.name}'s latency is {round(self.bot.latency * 1000)}ms."
        )

async def setup(bot: RimsaBot):
    await bot.add_cog(General(bot))
