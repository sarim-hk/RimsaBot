import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot
from utils.database import Database
from utils.reactions import react_info_to_chart, react_leaderboard_to_chart
from typing import Any, cast

MAX_ENTRIES = 10

class Reactions(commands.GroupCog, group_name="react"):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.database = Database(bot.cfg)

    async def cog_unload(self):
        if self.database.connection.is_connected():
            self.database.connection.close()

    @app_commands.command(name="set_max_message_age", description="Set the maximum message age for reaction tracking.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def set_max_message_age(self, interaction: discord.Interaction, age: app_commands.Range[int, 1, 2147483647]):
        if interaction.guild_id is None:
            return
        
        self.database.set_max_message_age(age, interaction.guild_id)

        await interaction.response.send_message(f"The maximum message has been set to `{age}` seconds for reaction tracking.")

    @app_commands.command(name="info", description="Get the reaction info for a given user.")
    async def info(self, interaction: discord.Interaction, user: discord.Member | None = None):
        if interaction.guild_id is None:
            return

        user = user or cast(discord.Member, interaction.user)
        react_info = self.database.get_user_reactions_received(user.id, interaction.guild_id)
        react_info = react_info[:MAX_ENTRIES]

        chart = react_info_to_chart(react_info)
        await interaction.response.send_message(chart)

    @app_commands.command(name="leaderboard", description="Get the leaderboard for a given emoji.")
    async def leaderboard(self, interaction: discord.Interaction, emoji: str):
        if interaction.guild_id is None or interaction.guild is None:
            return

        await interaction.response.defer()

        parsed_emoji = discord.PartialEmoji.from_str(emoji)
        leaderboard_info = self.database.get_emoji_leaderboard(parsed_emoji.name, parsed_emoji.id, interaction.guild_id)
        leaderboard_info = leaderboard_info[:MAX_ENTRIES]

        valid_leaderboard: list[dict[str, Any]] = []
        for entry in leaderboard_info:
            author_id = int(entry["AuthorID"])
            member = interaction.guild.get_member(author_id)
            
            if member is None:
                try:
                    member = await interaction.guild.fetch_member(author_id)
                except discord.HTTPException:
                    continue

            entry["AuthorName"] = member.name
            valid_leaderboard.append(entry)

        chart = react_leaderboard_to_chart(valid_leaderboard)
        await interaction.followup.send(chart)

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
        if payload.guild_id is None:   # doesnt return message_author_id on removal
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
