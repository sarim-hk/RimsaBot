import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot
from utils.database import Database

class Reactions(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.database = Database(bot.cfg)

    @app_commands.command(name="set_max_message_age", description="Set the maximum message age for reaction tracking.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def set_max_message_age(self, interaction: discord.Interaction, age: app_commands.Range[int, 1, 2147483647]):
        if interaction.guild_id is None:
            return
        
        self.database.set_max_message_age(age, interaction.guild_id)

        await interaction.response.send_message(f"The maximum message has been set to `{age}` seconds for reaction tracking.")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.message_author_id is None or payload.guild_id is None or (payload.message_author_id == payload.user_id):
            return

        self.database.insert_guild_config(payload.guild_id)

        message_created_at = discord.utils.snowflake_time(payload.message_id)
        message_age = discord.utils.utcnow() - message_created_at
        max_age = self.database.get_max_message_age(payload.guild_id)
        if message_age.total_seconds() > max_age:
            return

        self.database.insert_reaction(payload)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        if payload.message_author_id is None or payload.guild_id is None or (payload.message_author_id == payload.user_id):
            return

        self.database.insert_guild_config(payload.guild_id)

        message_created_at = discord.utils.snowflake_time(payload.message_id)
        message_age = discord.utils.utcnow() - message_created_at
        max_age = self.database.get_max_message_age(payload.guild_id)
        if message_age.total_seconds() > max_age:
            return
        
        self.database.remove_reaction(payload)

async def setup(bot: RimsaBot):
    await bot.add_cog(Reactions(bot))
