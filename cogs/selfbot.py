
import json
import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot
from typing import Any

class SelfBot(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.cfg = bot.cfg

    @app_commands.command(name="restream", description="Restream a Discord stream.")
    @app_commands.describe(user="The user's stream to restream.")
    @app_commands.guild_only()
    async def restream(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=True)

        if not user.voice or not user.voice.channel:
            await interaction.followup.send(
                f"{user.display_name} is not in a voice channel.",
                ephemeral=True,
            )
            return

        if not user.voice.self_stream:
            await interaction.followup.send(
                f"{user.display_name} is in voice, but is not currently streaming.",
                ephemeral=True,
            )
            return

        selfbot_user = await self.bot.fetch_user(int(self.cfg["SELFBOT_USER_ID"]))
        payload: dict[str, Any] = {
            "command": interaction.command.qualified_name if interaction.command else None,
            "target_user": {
                "id": user.id,
                "name": str(user),
                "display_name": user.display_name,
                "voice_channel_id": user.voice.channel.id,
                "voice_channel_name": user.voice.channel.name,
            },
            "user": {
                "id": interaction.user.id,
                "name": str(interaction.user),
            },
            "guild": {
                "id": interaction.guild.id if interaction.guild else None,
                "name": interaction.guild.name if interaction.guild else None,
            },
            "channel": {
                "id": interaction.channel.id if interaction.channel else None,
                "name": getattr(interaction.channel, "name", None),
            },
        }

        message = f"```json\n{json.dumps(payload, indent=2)}\n```"

        try:
            await selfbot_user.send(message)
        except discord.Forbidden:
            await interaction.followup.send(
                "Couldn't DM the selfbot user. The bot and that user need to share a server first.",
                ephemeral=True,
            )
            return

        await interaction.followup.send("Restream request sent to selfbot.", ephemeral=True)

async def setup(bot: RimsaBot):
    await bot.add_cog(SelfBot(bot))
