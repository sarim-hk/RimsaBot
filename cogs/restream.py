import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot
from utils.relay import RelayError, RelayManager

# Discord rejects messages above 2000 characters; status text includes the
# probe summary, so clip defensively.
MESSAGE_LIMIT = 1990


def _clip(text: str) -> str:
    return text if len(text) <= MESSAGE_LIMIT else text[: MESSAGE_LIMIT - 1] + "…"


class Restream(commands.Cog):
    # Subcommands cannot carry their own guild_only/default_permissions
    # (Discord limitation); both live on the group and apply to every child.
    restream = app_commands.Group(
        name="restream",
        description="Control the active re-stream.",
        guild_only=True,
        default_permissions=discord.Permissions(manage_guild=True),
    )

    def __init__(self, bot: RimsaBot):
        self.bot = bot
        # Account tokens come from config.json (empty watcher = dry-run mode).
        self.manager = RelayManager(
            bot.cfg.get("WATCHER_TOKEN") or None,
            bot.cfg.get("SENDER_TOKEN") or None,
        )

    @restream.command(
        name="start",
        description="Re-stream someone's stream from their voice channel into yours.",
    )
    @app_commands.describe(user="The person who is streaming.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def start(self, interaction: discord.Interaction, user: discord.Member):
        invoker = interaction.user
        assert isinstance(invoker, discord.Member)  # guild_only guarantees this

        dest = invoker.voice.channel if invoker.voice else None
        if dest is None:
            await interaction.response.send_message(
                "Join the voice channel you want the stream shown in first.",
                ephemeral=True,
            )
            return

        target_voice = user.voice
        source = target_voice.channel if target_voice else None
        if target_voice is None or source is None:
            await interaction.response.send_message(
                f"{user.display_name} isn't in a voice channel.", ephemeral=True
            )
            return

        if not target_voice.self_stream:
            await interaction.response.send_message(
                f"{user.display_name} isn't streaming right now.", ephemeral=True
            )
            return

        """
        # CHECK IF ALREADY IN CHANNEL - DISABLED FOR DEBUGGING
        if source.id == dest.id:
            await interaction.response.send_message(
                "You're already in that channel, just watch the stream there.",
                ephemeral=True,
            )
            return
        """

        # Logging in and watching takes a few seconds.
        await interaction.response.defer(thinking=True, ephemeral=True)

        try:
            session = await self.manager.start(
                guild_id=interaction.guild_id or 0,
                source_channel_id=source.id,
                dest_channel_id=dest.id,
                target_user_id=user.id,
                started_by=invoker.id,
            )
        except RelayError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        if self.manager.sender_status is not None:
            await interaction.followup.send(
                f"The sender account is live in {dest.mention} with its own Go Live.\n"
                f"-# Open the sender's Go Live in {dest.mention}. As soon as you do, "
                f"the relay joins {user.mention}'s stream in {source.mention} and "
                f"playback starts with a fresh keyframe. Check `/restream status`.",
                ephemeral=True,
            )
        elif session.stream is not None:
            await interaction.followup.send(
                f"Watcher is in {source.mention} and attached to {user.mention}'s "
                f"stream (region `{session.stream.region}`).\n"
                f"-# Milestone 3b: probing the stream over UDP and trying to join "
                f"its DAVE group to decrypt frames (~40s). Check `/restream status`. "
                f"Nothing is relayed to {dest.mention} yet.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                f"Would relay {user.mention}'s stream from {source.mention} "
                f"to {dest.mention}.\n"
                f"-# No watcher configured (set `WATCHER_TOKEN` in config.json), "
                f"so this was a dry run.",
                ephemeral=True,
            )

    @restream.command(name="stop", description="Stop the active re-stream.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stop(self, interaction: discord.Interaction):
        if self.manager.active is None:
            await interaction.response.send_message(
                "No relay is running.", ephemeral=True
            )
            return
        # Closing the accounts (ending the Go Live and leaving the channels)
        # can take a few seconds; respond within Discord's 3 s window first.
        await interaction.response.defer(ephemeral=True)
        await self.manager.stop()
        await interaction.followup.send("Relay stopped.", ephemeral=True)

    @restream.command(name="status", description="Show the active re-stream.")
    async def status(self, interaction: discord.Interaction):
        s = self.manager.active
        if s is None:
            msg = "No relay is running."
            if self.manager.last_ended is not None:
                last, reason, when = self.manager.last_ended
                msg += (
                    f"\nLast relay ended on its own <t:{int(when)}:R>: `{reason}`"
                    f"\nMedia probe: `{last.media_status}`"
                )
            await interaction.response.send_message(_clip(msg), ephemeral=True)
            return
        extra = ""
        if s.watcher_status != "attached":
            extra += f"\nWatcher: `{s.watcher_status}`"
        sender = self.manager.sender_status
        if sender is not None:
            extra += f"\nSender: `{sender}`"
        await interaction.response.send_message(
            _clip(
                f"Relaying <@{s.target_user_id}> from <#{s.source_channel_id}> "
                f"to <#{s.dest_channel_id}> (started <t:{int(s.started_at)}:R>).\n"
                f"Media probe: `{s.media_status}`{extra}"
            ),
            allowed_mentions=discord.AllowedMentions.none(),
            ephemeral=True,
        )

    async def cog_unload(self):
        await self.manager.stop()


async def setup(bot: RimsaBot):
    await bot.add_cog(Restream(bot))
