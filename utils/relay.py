from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from utils.stream_voice import StreamProbe
from utils.user_gateway import DEFAULT_GATEWAY_URL, GatewayError, StreamInfo, Watcher
from utils.voice_sender import Sender, SenderError

log = logging.getLogger(__name__)


class RelayError(Exception):
    """A problem that is safe to show to the person who ran the command."""


@dataclass
class RelaySession:
    guild_id: int
    source_channel_id: int   # channel A: where the stream is
    dest_channel_id: int     # channel B: where it should be re-streamed
    target_user_id: int      # the person who is streaming in A
    started_by: int
    started_at: float = field(default_factory=time.time)
    stream: StreamInfo | None = None  # set once the watcher is attached
    media_status: str = "not started"  # milestone 3a probe result
    watcher_status: str = "attached"   # updated if the watcher ends before the probe


class RelayManager:
    """Owns the (single) active relay: watcher(A) -> decrypt -> sender(B)."""

    # How long the sender's Go Live may sit without a viewer after /restream.
    RELAY_VIEWER_TIMEOUT = 300.0
    # Retry interval for RTCP PLI keyframe requests until one gets through.
    KEYFRAME_PLI_INTERVAL = 1.5

    def __init__(
        self,
        watcher_token: str | None = None,
        sender_token: str | None = None,
        *,
        gateway_url: str = DEFAULT_GATEWAY_URL,
    ) -> None:
        self._watcher_token = watcher_token
        self._sender_token = sender_token
        self._gateway_url = gateway_url
        self._active: RelaySession | None = None
        self._watcher: Watcher | None = None
        self._probe: StreamProbe | None = None
        self._sender: Sender | None = None
        self._probe_finished = False
        # (session, why it ended, when) for the last relay that ended on its own
        self.last_ended: tuple[RelaySession, str, float] | None = None
        self._bg: set[asyncio.Task] = set()

    @property
    def active(self) -> RelaySession | None:
        return self._active

    @property
    def watcher_configured(self) -> bool:
        return bool(self._watcher_token)

    @property
    def has_sender(self) -> bool:
        return bool(self._sender_token)

    @property
    def sender_status(self) -> str | None:
        return self._sender.status() if self._sender is not None else None

    async def start(
        self,
        *,
        guild_id: int,
        source_channel_id: int,
        dest_channel_id: int,
        target_user_id: int,
        started_by: int,
    ) -> RelaySession:
        if self._active is not None:
            raise RelayError("A relay is already running. Use `/restream stop` first.")
        if self._sender_token and not self._watcher_token:
            raise RelayError("SENDER_TOKEN is set without WATCHER_TOKEN; both are required.")

        session = RelaySession(
            guild_id=guild_id,
            source_channel_id=source_channel_id,
            dest_channel_id=dest_channel_id,
            target_user_id=target_user_id,
            started_by=started_by,
            watcher_status="not started",
        )

        sender = None
        if self._sender_token:
            sender = Sender(
                self._sender_token,
                guild_id=guild_id,
                channel_id=dest_channel_id,
                on_end=self._on_sender_ended,
            )
            try:
                await sender.start()
            except SenderError as e:
                await sender.close()
                raise RelayError(f"Sender failed: {e}") from e

        self._active = session
        self._sender = sender
        self._watcher = None
        self._probe = None
        self._probe_finished = False

        if sender is not None:
            # Sender first. Discord streams emit a keyframe when a viewer
            # joins, so the watcher may only attach to A once the sender's Go
            # Live has a viewer. Attaching earlier spends the only keyframe on
            # frames the sender cannot encrypt yet, and the viewer waits
            # forever for an IDR.
            session.media_status = "waiting for the sender's Go Live to be opened"
            session.watcher_status = "waiting for the viewer"
            self._spawn(self._watch_after_sender(session))
            return session

        # Watcher-only mode: the milestone 3 diagnostic (20 s window).
        if self._watcher_token:
            try:
                await self._attach_watcher(session)
            except GatewayError as e:
                self._active = None
                raise RelayError(f"Watcher failed: {e}") from e
            if session.stream is not None:
                self._probe = StreamProbe(session.stream, on_frame=self._on_frame)
                self._spawn(self._run_probe(session, self._probe, 20.0))
        else:
            session.media_status = "dry run (no WATCHER_TOKEN)"
            session.watcher_status = "not configured"
        return session

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _attach_watcher(self, session: RelaySession) -> StreamInfo:
        """Join A with the watcher account and attach to the target's stream."""
        assert self._watcher_token
        watcher = Watcher(
            self._watcher_token,
            gateway_url=self._gateway_url,
            on_end=self._on_stream_ended,
        )
        try:
            stream = await watcher.watch(
                guild_id=session.guild_id,
                channel_id=session.source_channel_id,
                target_user_id=session.target_user_id,
            )
        except GatewayError:
            await watcher.close()
            raise
        self._watcher = watcher
        session.stream = stream
        session.watcher_status = "attached"
        return stream

    async def _watch_after_sender(self, session: RelaySession) -> None:
        """Wait for a viewer on B, then join A and start relaying.

        With no viewer on the sender's Go Live there is no MLS group and no
        encryptor ratchet; waiting here also means our own join to A is what
        triggers the streamer's first keyframe, just as the viewer needs it.
        """
        sender = self._sender
        assert sender is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.RELAY_VIEWER_TIMEOUT
        while not sender.can_send:
            if self._active is not session:
                return
            if loop.time() > deadline:
                self._finish(session, "no viewer opened the sender's Go Live in time")
                return
            await asyncio.sleep(0.5)
        if self._active is not session:
            return
        log.info("sender: viewer ready; attaching the watcher to the source stream")
        try:
            stream = await self._attach_watcher(session)
        except GatewayError as e:
            log.warning("watcher failed after the sender was ready: %s", e)
            self._finish(session, f"watcher failed: {e}")
            return
        if self._active is not session:
            # The relay was stopped while the watcher was connecting.
            watcher, self._watcher = self._watcher, None
            if watcher is not None:
                await watcher.close()
            return
        probe = self._probe = StreamProbe(stream, on_frame=self._on_frame)
        # A viewer PLI on B asks for a fresh IDR; request one from A.
        sender.on_keyframe_request = probe.request_keyframe
        self._spawn(self._run_probe(session, probe, None))
        self._spawn(self._ensure_keyframe(session, probe, sender))

    async def _ensure_keyframe(
        self, session: RelaySession, probe: StreamProbe, sender: Sender
    ) -> None:
        """Request an IDR from A until one reaches the sender.

        Our viewer join normally triggers a keyframe, but it can arrive before
        the probe has installed its MLS ratchet. An RTCP PLI (what official
        viewers send) makes the streamer emit a fresh one.
        """
        for _ in range(40):
            if self._active is not session or sender.keyframes_sent:
                return
            probe.request_keyframe()
            await asyncio.sleep(self.KEYFRAME_PLI_INTERVAL)

    def _on_frame(self, kind: str, plaintext: bytes, timestamp: int) -> None:
        sender = self._sender
        if sender is None:
            return
        if kind == "audio":
            sender.send_audio(plaintext, timestamp)
        else:
            sender.send_video(plaintext, timestamp)

    async def _run_probe(
        self, session: RelaySession, probe: StreamProbe, listen: float | None
    ) -> None:
        session.media_status = "probing"
        try:
            result = await probe.run(listen=listen)
        except Exception as e:
            log.exception("stream probe crashed")
            result = f"probe crashed: {type(e).__name__}"
        session.media_status = result
        self._probe_finished = True
        log.info("stream probe result: %s", result)
        if self._active is not session:
            return
        if listen is None:
            # Relay mode: the stream socket ending, or media stopping, ends
            # the relay.
            if probe.idle_expired:
                reason = f"no media from the source for {probe.idle_timeout:.0f}s"
            else:
                reason = "stream socket closed"
            self._finish(session, reason)
        elif self._watcher is None:
            # The watcher died while the probe was still running; now that the
            # probe is done there is nothing left to observe.
            self._finish(session, session.watcher_status)

    async def stop(self) -> RelaySession | None:
        session, self._active = self._active, None
        watcher, self._watcher = self._watcher, None
        probe, self._probe = self._probe, None
        sender, self._sender = self._sender, None
        self._probe_finished = False
        if sender is not None:
            sender.on_keyframe_request = None
            sender.on_end = None
        if probe is not None:
            await probe.close()
        if sender is not None:
            await sender.close()
        if watcher is not None:
            await watcher.close()
        return session

    def _on_sender_ended(self, reason: str) -> None:
        """Called by the sender if its media path (B's stream socket) dies."""
        sender, self._sender = self._sender, None
        if sender is None:
            return
        sender.on_end = None
        sender.on_keyframe_request = None
        log.info("Sender ended: %s", reason)
        self._close_later(sender)

        session = self._active
        if session is None:
            return
        self._finish(session, f"sender ended: {reason}")

    def _on_stream_ended(self) -> None:
        """Called by the watcher when the stream stops or its connection drops."""
        watcher, self._watcher = self._watcher, None
        if watcher is None:
            return
        kind = watcher.end_kind or "connection_lost"
        reason = watcher.end_reason or "unknown"
        log.info("Watcher ended (%s): %s", kind, reason)
        self._close_later(watcher)

        session = self._active
        if session is None:
            return
        session.watcher_status = f"{kind}: {reason}"

        # A dropped *main gateway* says nothing about the stream voice socket:
        # that connection and its short-lived token are separate. Keep the probe
        # running there, since it is the only thing left that can tell us
        # whether media still flows. Anything else (stream ended, watcher left
        # the channel) means the media path is gone too.
        still_probing = (
            kind == "connection_lost"
            and self._probe is not None
            and not self._probe_finished
        )
        if not still_probing:
            self._finish(session, reason)

    def _finish(self, session: RelaySession, reason: str) -> None:
        """Clear the active relay and close whatever is still open."""
        if self._active is not session:
            return
        self.last_ended = (session, reason, time.time())
        self._active = None
        probe, self._probe = self._probe, None
        watcher, self._watcher = self._watcher, None
        sender, self._sender = self._sender, None
        for closer in (probe, watcher, sender):
            if closer is not None:
                self._close_later(closer)

    def _close_later(self, closer: Watcher | StreamProbe | Sender) -> None:
        task = asyncio.create_task(closer.close())
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
