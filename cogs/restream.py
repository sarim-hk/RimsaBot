import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot
from utils.relay import RelayError, RelayManager, RelaySession

# Discord rejects messages above 2000 characters; status text includes the
# probe summary, so clip defensively.
MESSAGE_LIMIT = 1990

# How the relay can end, in plain words. Matched against the internal reason,
# which stays available behind `/restream status debug:True`.
FRIENDLY_ENDINGS = (
    ("no viewer opened", "nobody opened the sender's stream within 5 minutes"),
    ("sender ended", "the sender account lost its stream connection"),
    ("no media from the source", "the source stopped sending video"),
    ("stream socket closed", "the source stream stopped"),
    ("stream_delete", "the source stream stopped"),
    ("watcher was", "the watcher account left the source channel"),
    ("watcher failed", "the relay couldn't join the source stream"),
    ("gateway connection closed", "the watcher's connection to Discord dropped"),
    ("probe crashed", "the relay hit an internal error"),
    ("sender failed", "the sender account couldn't connect"),
)

def _clip(text: str) -> str:
    return text if len(text) <= MESSAGE_LIMIT else text[: MESSAGE_LIMIT - 1] + "…"

def _friendly_ending(reason: str) -> str:
    low = reason.lower()
    for needle, text in FRIENDLY_ENDINGS:
        if needle in low:
            return text
    return "it ended unexpectedly"

def _where(s: RelaySession) -> str:
    if s.source_channel_id == s.dest_channel_id:
        return f"in <#{s.source_channel_id}>"
    return f"from <#{s.source_channel_id}> into <#{s.dest_channel_id}>"

class Restream(commands.Cog):
    # Subcommands cannot carry their own guild_only/default_permissions
    # (Discord limitation); both live on the group and apply to every child.
    restream = app_commands.Group(name="restream", description="Control the active re-stream.", guild_only=True, default_permissions=discord.Permissions(manage_guild=True))

    def __init__(self, bot: RimsaBot):
        self.bot = bot
        # Account tokens come from config.json (empty watcher = dry-run mode).
        self.manager = RelayManager(
            bot.cfg.get("WATCHER_TOKEN") or None,
            bot.cfg.get("SENDER_TOKEN") or None,
        )

    @restream.command(name="start", description="Relay someone's stream into your voice channel.")
    @app_commands.describe(user="The person who is streaming.", debug="Skip the same-channel check (for testing in one channel).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def start(self, interaction: discord.Interaction, user: discord.Member, debug: bool = False):
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
                f"{user.mention} isn't in a voice channel.",
                allowed_mentions=discord.AllowedMentions.none(),
                ephemeral=True,
            )
            return

        if not target_voice.self_stream:
            await interaction.response.send_message(
                f"{user.mention} isn't streaming right now.",
                allowed_mentions=discord.AllowedMentions.none(),
                ephemeral=True,
            )
            return

        # Already in the channel? With debug on, allow it: testing the relay
        # inside a single channel is the point of that flag.
        if not debug and source.id == dest.id:
            await interaction.response.send_message(
                "You're already in that channel, just watch the stream there.\n"
                "-# Use `debug:True` to relay it into the same channel anyway.",
                ephemeral=True,
            )
            return

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
            lines = [
                f"{user.mention}'s stream will play in {dest.mention}.",
                "-# Check progress any time with `/restream status`.",
            ]
        elif session.stream is not None:
            lines = [
                f"Watching {user.mention}'s stream in {source.mention}.",
                "-# Collecting diagnostics for about 40 seconds.",
            ]
            if debug:
                lines.append(f"-# Region `{session.stream.region}`.")
        else:
            lines = [
                "Dry run — no watcher account is configured, so nothing was relayed.",
                "-# Set `WATCHER_TOKEN` in `config.json` to enable the relay.",
            ]
        await interaction.followup.send(
            "\n".join(lines),
            allowed_mentions=discord.AllowedMentions.none(),
            ephemeral=True,
        )

    @restream.command(name="stop", description="Stop the active re-stream.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stop(self, interaction: discord.Interaction):
        if self.manager.active is None:
            await interaction.response.send_message(
                "No restream is running.", ephemeral=True
            )
            return
        # Closing the accounts (ending the stream and leaving the channels)
        # can take a few seconds; respond within Discord's 3 s window first.
        await interaction.response.defer(ephemeral=True)
        await self.manager.stop()
        await interaction.followup.send("Stopped.", ephemeral=True)

    @restream.command(name="status", description="Show how the active re-stream is doing.")
    @app_commands.describe(debug="Also show the raw diagnostic counters.")
    async def status(self, interaction: discord.Interaction, debug: bool = False):
        s = self.manager.active
        if s is None:
            lines = ["Nothing is being relayed right now."]
            if self.manager.last_ended is not None:
                last, reason, when = self.manager.last_ended
                lines.append(
                    f"-# The last restream of <@{last.target_user_id}> "
                    f"{_where(last)} ended <t:{int(when)}:R> — "
                    f"{_friendly_ending(reason)}. Start another with "
                    f"`/restream start`."
                )
                if debug:
                    lines.append(f"-# Technical: `{reason}`")
                    lines.append(f"-# Probe: `{last.media_status}`")
            else:
                lines.append("-# Start one with `/restream start`.")
            await interaction.response.send_message(
                _clip("\n".join(lines)),
                allowed_mentions=discord.AllowedMentions.none(),
                ephemeral=True,
            )
            return

        lines = [
            f"Restreaming <@{s.target_user_id}> {_where(s)}",
        ]
        sender = self.manager.sender_stats()
        watcher_ok = s.watcher_status == "attached"

        if sender is not None:
            if not sender["viewer_ready"]:
                state = "Waiting for the stream to be opened."
            elif sender["video_sent"]:
                state = "Playback is running."
            elif self.manager.probe_stats() is None:
                state = "Connecting to the source stream…"
            else:
                state = "Waiting for the first video frame."
        elif self.manager.probe_stats() is None:
            state = "Connecting to the source stream…"
        else:
            state = (
                "Collecting diagnostics — nothing is forwarded to the "
                "destination channel."
            )
        lines.append(f"-# Started <t:{int(s.started_at)}:R>. {state}")

        if debug:
            if s.stream is not None:
                lines.append(f"-# Region: `{s.stream.region}`")
            lines.append(f"-# Probe: `{s.media_status}`")
            sender_text = self.manager.sender_status
            if sender_text is not None:
                lines.append(f"-# Sender: `{sender_text}`")
            if not watcher_ok:
                lines.append(f"-# Watcher: `{s.watcher_status}`")

        await interaction.response.send_message(
            _clip("\n".join(lines)),
            allowed_mentions=discord.AllowedMentions.none(),
            ephemeral=True,
        )

    async def cog_unload(self):
        await self.manager.stop()

async def setup(bot: RimsaBot):
    restream_guild_decorator = app_commands.guilds(int(bot.cfg["RESTREAM_GUILD_ID"]))
    cog = Restream(bot)
    for command in cog.get_app_commands():
        restream_guild_decorator(command)

    await bot.add_cog(cog)
