"""Milestone 4: sender side of the relay.

Logs in as the sender account, joins voice channel B, starts a Go Live in
that channel, joins the stream's MLS group, and sends media frames
re-encrypted with that group's key ratchet.

Media path for each frame: DAVE encrypt -> H264/Opus packetize -> transport
AEAD (aead_aes256_gcm_rtpsize) -> UDP to the stream server.

The stream group only exists once a viewer opens the Go Live; until our own
key ratchet is installed, frames are counted as dropped.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import struct
import time
import uuid
import zlib
from dataclasses import dataclass
from typing import Any, Callable

import aiohttp

from utils import dave_media
from utils.dave_media import DaveState, TransportCrypto
from utils.user_gateway import (
    BROWSER_USER_AGENT,
    DEFAULT_GATEWAY_URL,
    GATEWAY_HEADERS,
    GATEWAY_ORIGIN,
    WS_COMPRESS,
    ZLIB_SUFFIX,
    build_identify,
    clean_token,
)

log = logging.getLogger(__name__)

VOICE_GATEWAY_VERSION = 9
OPUS_SILENCE = b"\xf8\xff\xfe"
OPUS_PAYLOAD_TYPE = 120
H264_PAYLOAD_TYPE = 101
RTX_PAYLOAD_TYPE = 102
PREFERRED_MODES = ("aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize")
MAX_RTP_PAYLOAD = 1100  # keep RTP + transport overhead under the MTU
AUDIO_FRAME_SAMPLES = 960  # 20 ms of Opus at 48 kHz
VIDEO_FRAME_TICKS = 3000  # 30 fps at 90 kHz
SILENCE_BYTES = 3


class SenderError(Exception):
    """A sender failure whose message is safe to show."""


@dataclass
class StreamTarget:
    stream_key: str
    rtc_server_id: str
    rtc_channel_id: str
    region: str
    endpoint: str
    token: str


class _Udp(asyncio.DatagramProtocol):
    def __init__(self, on_packet=None) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.on_packet = on_packet
        self.discovery: asyncio.Future[tuple[str, int]] = (
            asyncio.get_running_loop().create_future()
        )

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        if not self.discovery.done() and len(data) >= 74 and data[:2] == b"\x00\x02":
            ip = data[8:72].split(b"\x00", 1)[0].decode()
            port = struct.unpack(">H", data[72:74])[0]
            self.discovery.set_result((ip, port))
            return
        if self.on_packet is not None:
            self.on_packet(data)


def split_annexb(data: bytes) -> list[bytes]:
    """Split an Annex-B access unit into NAL units (start codes removed)."""
    starts: list[tuple[int, int]] = []
    i = 0
    n = len(data)
    while i + 3 <= n:
        if data[i : i + 4] == b"\x00\x00\x00\x01":
            starts.append((i, 4))
            i += 4
        elif data[i : i + 3] == b"\x00\x00\x01":
            starts.append((i, 3))
            i += 3
        else:
            i += 1
    nals: list[bytes] = []
    for index, (pos, size) in enumerate(starts):
        start = pos + size
        end = starts[index + 1][0] if index + 1 < len(starts) else n
        if end > start:
            nals.append(data[start:end])
    return nals


def packetize_h264(nals: list[bytes], max_payload: int = MAX_RTP_PAYLOAD) -> list[tuple[bytes, bool]]:
    """Turn NAL units into RTP payloads; the marker goes on the very last one."""
    packets: list[tuple[bytes, bool]] = []
    for index, nal in enumerate(nals):
        last_nal = index == len(nals) - 1
        if len(nal) <= max_payload:
            packets.append((nal, last_nal))
            continue
        header = nal[0]
        body = nal[1:]
        chunk = max(1, max_payload - 2)
        pos = 0
        while pos < len(body):
            first = pos == 0
            last = pos + chunk >= len(body)
            fu_indicator = (header & 0xE0) | 28
            fu_header = (0x80 if first else 0) | (0x40 if last else 0) | (header & 0x1F)
            packets.append(
                (bytes([fu_indicator, fu_header]) + body[pos : pos + chunk], last_nal and last)
            )
            pos += chunk
    return packets


class Sender:
    def __init__(
        self,
        token: str,
        *,
        guild_id: int,
        channel_id: int,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        on_end: Callable[[str], None] | None = None,
    ) -> None:
        self._token = clean_token(token)
        self._guild_id = int(guild_id)
        self._channel_id = int(channel_id)  # channel B
        self._gateway_url = gateway_url
        self._launch_id = str(uuid.uuid4())
        # Set by the relay: called once if the media path dies on its own.
        self.on_end = on_end
        self._end_signalled = False

        self._http: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._inflater = zlib.decompressobj()
        self._buf = bytearray()
        self._seq: int | None = None
        self._tasks: list[asyncio.Task] = []
        self._closing = False
        self._futures: dict[str, asyncio.Future] = {}

        self.user_id = ""
        self.session_id = ""
        self.voice_session_id = ""
        self.stream_key = ""
        self.target: StreamTarget | None = None

        self._voice_ws: aiohttp.ClientWebSocketResponse | None = None
        self._voice_seq = -1
        self._voice_tasks: list[asyncio.Task] = []
        self._udp: _Udp | None = None
        self._crypto: TransportCrypto | None = None
        self._dave: DaveState | None = None
        self.encryptor: Any | None = None

        self.audio_ssrc = 0
        self.video_ssrc = random.getrandbits(32)
        self.rtx_ssrc = random.getrandbits(32)
        self._counter = random.getrandbits(32)
        self._audio_seq = random.getrandbits(16)
        self._audio_ts = random.getrandbits(32)
        self._video_seq = random.getrandbits(16)
        self._video_ts = random.getrandbits(32)

        self.audio_sent = 0
        self.video_sent = 0
        self.keyframes_sent = 0
        self.packets_sent = 0
        self.bytes_sent = 0
        self.dropped = 0
        self.rtcp_received = 0
        self.pli_requests = 0
        self._udp_logged = 0
        # Set by the relay: ask A for a fresh IDR when the viewer needs one.
        self.on_keyframe_request: Callable[[], Any] | None = None
        self._last_pli_at = 0.0
        # Each kind keeps an offset and the last source timestamp so outgoing
        # RTP follows A's real pacing instead of a fixed frame rate.
        self._audio_ts_offset: int | None = None
        self._video_ts_offset: int | None = None
        self._audio_src_ts: int | None = None
        self._video_src_ts: int | None = None
        self.state = "starting"

    # ----------------------------------------------------------------- public

    async def start(self) -> None:
        """Log in, join B, create the Go Live and join its stream voice socket."""
        self._futures = {
            name: asyncio.get_running_loop().create_future()
            for name in (
                "ready", "voice", "stream_create", "stream_server",
                "voice_ready", "session_description",
            )
        }
        await self._connect_gateway()
        await self._join_voice()
        await self._create_stream()
        await self._connect_stream_voice()
        self.state = "waiting for viewer"
        log.info(
            "sender: live in channel %s as %s (stream %s)",
            self._channel_id, self.user_id, self.stream_key,
        )

    @property
    def can_send(self) -> bool:
        return (
            self.encryptor is not None
            and self._udp is not None
            and self._crypto is not None
            and self.encryptor.has_key_ratchet()
        )

    def send_audio(self, opus: bytes, source_ts: int | None = None) -> bool:
        encryptor = self.encryptor
        if encryptor is None or not self.can_send or not opus:
            self.dropped += 1
            return False
        if opus == OPUS_SILENCE:
            frame = opus  # protocol silence packets pass through unencrypted
        else:
            frame = encryptor.encrypt(
                dave_media.dave.MediaType.audio, self.audio_ssrc, opus
            )
            if frame is None:
                self.dropped += 1
                return False
        self._audio_ts = self._map_timestamp("audio", source_ts, AUDIO_FRAME_SAMPLES)
        self._send_rtp(
            self.audio_ssrc, OPUS_PAYLOAD_TYPE, self._audio_seq, self._audio_ts,
            frame, marker=False,
        )
        self._audio_seq = (self._audio_seq + 1) & 0xFFFF
        self.audio_sent += 1
        self.state = "relaying"
        return True

    def send_video(self, annexb: bytes, source_ts: int | None = None) -> bool:
        encryptor = self.encryptor
        if encryptor is None or not self.can_send or len(annexb) < 5:
            self.dropped += 1
            return False
        # The plaintext NAL headers are authoritative for keyframe detection;
        # the protocol frame's headers should be visible too, but there is no
        # reason to parse them for this.
        plain_nals = split_annexb(annexb)
        frame = encryptor.encrypt(
            dave_media.dave.MediaType.video, self.video_ssrc, annexb
        )
        if frame is None:
            self.dropped += 1
            return False
        nals = split_annexb(frame)
        if not nals:
            self.dropped += 1
            return False
        self._video_ts = self._map_timestamp("video", source_ts, VIDEO_FRAME_TICKS)
        for payload, marker in packetize_h264(nals):
            self._send_rtp(
                self.video_ssrc, H264_PAYLOAD_TYPE, self._video_seq, self._video_ts,
                payload, marker=marker,
            )
            self._video_seq = (self._video_seq + 1) & 0xFFFF
        self.video_sent += 1
        if any((nal[0] & 0x1F) == 5 for nal in plain_nals):
            self.keyframes_sent += 1
            if self.keyframes_sent == 1:
                log.info("sender: first keyframe forwarded (%d B)", len(annexb))
        self.state = "relaying"
        return True

    def status(self) -> str:
        return (
            f"{self.state}; audio {self.audio_sent}, video {self.video_sent} "
            f"(keys {self.keyframes_sent}), dropped {self.dropped}, "
            f"rtcp {self.rtcp_received}, pli {self.pli_requests}"
        )

    async def close(self) -> None:
        self._closing = True
        self.on_end = None
        await self._leave_stream()
        current = asyncio.current_task()
        for task in self._tasks + self._voice_tasks:
            if task is not current:
                task.cancel()
        if self._voice_ws is not None and not self._voice_ws.closed:
            await self._voice_ws.close()
        if self._udp is not None and self._udp.transport is not None:
            self._udp.transport.close()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._http is not None:
            await self._http.close()

    async def _leave_stream(self) -> None:
        """End the Go Live and leave B explicitly before dropping the sockets.

        Closing the stream voice socket ends the stream too, but only after
        Discord notices; op19 makes B's stream disappear at once and op4
        takes the sender account out of the channel.
        """
        ws = self._ws
        if ws is None or ws.closed:
            return
        try:
            if self.stream_key:
                await asyncio.wait_for(
                    self._send(19, {"stream_key": self.stream_key}), 2.0
                )
            await asyncio.wait_for(
                self._send(4, {
                    "guild_id": str(self._guild_id),
                    "channel_id": None,
                    "self_mute": False,
                    "self_deaf": False,
                }),
                2.0,
            )
            log.info("sender: ended its Go Live and left the channel")
        except Exception:
            log.info("sender: explicit stream/voice leave failed; closing anyway")

    def _signal_end(self, reason: str) -> None:
        """Tell the relay the sender's media path is gone (once, and not on close)."""
        if self._closing or self._end_signalled or self.on_end is None:
            return
        self._end_signalled = True
        self.on_end(reason)

    # ---------------------------------------------------------------- gateway

    async def _token_status(self) -> int | str:
        assert self._http is not None
        try:
            async with self._http.get(
                "https://discord.com/api/v9/users/@me",
                headers={"Authorization": self._token},
            ) as response:
                return response.status
        except aiohttp.ClientError as e:
            return type(e).__name__

    async def _connect_gateway(self) -> None:
        self._http = aiohttp.ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
        status = await self._token_status()
        if status == 401:
            raise SenderError("Sender token is invalid (API returned 401).")
        if status == 403:
            raise SenderError("Sender account is locked or needs verification (403).")
        if status != 200:
            raise SenderError(f"Unexpected API status {status} checking the sender token.")
        try:
            self._ws = await self._http.ws_connect(
                self._gateway_url,
                max_msg_size=0,
                origin=GATEWAY_ORIGIN,
                headers=GATEWAY_HEADERS,
                compress=WS_COMPRESS,
            )
        except aiohttp.ClientError as e:
            raise SenderError(f"Couldn't reach the gateway: {e}") from e
        self._tasks.append(asyncio.create_task(self._read_loop()))
        await self._wait("ready", 20, "the sender to log in")

    async def _join_voice(self) -> None:
        await self._send(4, {
            "guild_id": str(self._guild_id),
            "channel_id": str(self._channel_id),
            "self_mute": False,
            "self_deaf": False,
            "self_video": False,
            "flags": 2,
        })
        await self._wait("voice", 20, "the sender to join the voice channel")

    async def _create_stream(self) -> None:
        self.stream_key = f"guild:{self._guild_id}:{self._channel_id}:{self.user_id}"
        await self._send(18, {
            "type": "guild",
            "guild_id": str(self._guild_id),
            "channel_id": str(self._channel_id),
            "preferred_region": None,
        })
        created = await self._wait("stream_create", 20, "the stream to be created")
        server = await self._wait("stream_server", 20, "the stream server")
        await self._send(22, {"stream_key": self.stream_key, "paused": False})
        self.target = StreamTarget(
            stream_key=self.stream_key,
            rtc_server_id=str(created["rtc_server_id"]),
            rtc_channel_id=str(created["rtc_channel_id"]),
            region=created.get("region", "?"),
            endpoint=server["endpoint"],
            token=server["token"],
        )

    def _note(self, event: str) -> None:
        log.info("sender gateway: %s", event)

    async def _send(self, op: int, d: Any) -> None:
        assert self._ws is not None
        await self._ws.send_str(json.dumps({"op": op, "d": d}))

    async def _wait(self, name: str, timeout: float, what: str) -> Any:
        try:
            return await asyncio.wait_for(asyncio.shield(self._futures[name]), timeout)
        except asyncio.TimeoutError:
            raise SenderError(f"Timed out waiting for {what}.") from None

    def _resolve(self, name: str, value: Any) -> None:
        future = self._futures.get(name)
        if future is not None and not future.done():
            future.set_result(value)

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for message in self._ws:
                if message.type == aiohttp.WSMsgType.BINARY:
                    self._buf.extend(message.data)
                    if message.data[-4:] != ZLIB_SUFFIX:
                        continue
                    payload = json.loads(self._inflater.decompress(bytes(self._buf)))
                    self._buf.clear()
                elif message.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(message.data)
                else:
                    continue
                await self._handle(payload)
        except Exception as e:
            log.warning("sender gateway loop error: %s", type(e).__name__)
        finally:
            if not self._closing:
                log.warning("sender gateway closed (code %s)", self._ws.close_code)
                if self._voice_ws is None or self._voice_ws.closed:
                    # No media socket either: there is nothing left to send on.
                    self._signal_end("gateway closed before the stream socket")
                else:
                    # Like the watcher: the main gateway is not the media path.
                    log.info("sender: stream socket still open; continuing")

    async def _handle(self, payload: dict) -> None:
        op = payload.get("op")
        if payload.get("s") is not None:
            self._seq = payload["s"]
        if op == 10:
            self._tasks.append(asyncio.create_task(self._heartbeat(payload["d"]["heartbeat_interval"])))
            await self._send(2, build_identify(self._token, self._launch_id))
        elif op == 1:
            await self._send(1, self._seq)
        elif op == 0:
            self._dispatch(payload["t"], payload.get("d") or {})

    def _dispatch(self, event: str, d: dict) -> None:
        if event == "READY":
            self.user_id = d["user"]["id"]
            self.session_id = d.get("session_id", "")
            self._resolve("ready", True)
        elif event == "VOICE_STATE_UPDATE":
            if d.get("user_id") != self.user_id:
                return
            if str(d.get("channel_id")) == str(self._channel_id):
                self.voice_session_id = d.get("session_id") or self.session_id
                self._resolve("voice", True)
        elif event == "STREAM_CREATE" and d.get("stream_key") == self.stream_key:
            self._resolve("stream_create", d)
        elif event == "STREAM_SERVER_UPDATE" and d.get("stream_key") == self.stream_key:
            self._resolve("stream_server", d)

    async def _heartbeat(self, interval_ms: int) -> None:
        interval = interval_ms / 1000
        await asyncio.sleep(interval * random.random())
        while True:
            await self._send(1, self._seq)
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------ voice

    async def _connect_stream_voice(self) -> None:
        assert self.target is not None and self._http is not None
        target = self.target
        if dave_media.available():
            self.encryptor = dave_media.dave.Encryptor()
            self._dave = DaveState(
                group_id=int(target.rtc_server_id) - 1,
                self_user_id=self.user_id,
                streamer_user_id=self.user_id,  # we are the sender of this group
                self_encryptor=self.encryptor,
            )
        host = target.endpoint.removeprefix("wss://").rstrip("/")
        url = f"wss://{host}/?v={VOICE_GATEWAY_VERSION}"
        try:
            self._voice_ws = await self._http.ws_connect(url, max_msg_size=0)
        except aiohttp.ClientError as e:
            raise SenderError(f"Couldn't reach the sender's stream server: {e}") from e
        self._voice_tasks.append(asyncio.create_task(self._voice_loop()))
        ready = await self._wait("voice_ready", 15, "the sender's stream voice server")

        mode = next((m for m in PREFERRED_MODES if m in ready.get("modes", [])), None)
        if mode is None:
            raise SenderError(f"No supported transport mode in {ready.get('modes')}")
        ip, port = await self._discover(ready["ip"], ready["port"], ready["ssrc"])

        self.audio_ssrc = int(ready["ssrc"])
        if self.encryptor is not None:
            self.encryptor.assign_ssrc_to_codec(self.audio_ssrc, dave_media.dave.Codec.opus)
            self.encryptor.assign_ssrc_to_codec(self.video_ssrc, dave_media.dave.Codec.h264)

        await self._voice_send(1, {
            "protocol": "udp",
            "data": {"address": ip, "port": port, "mode": mode},
            "codecs": [
                {"name": "opus", "type": "audio", "priority": 1000, "payload_type": OPUS_PAYLOAD_TYPE},
                {"name": "H264", "type": "video", "priority": 1000, "payload_type": H264_PAYLOAD_TYPE,
                 "rtx_payload_type": RTX_PAYLOAD_TYPE, "encode": True, "decode": False},
            ],
        })
        session = await self._wait("session_description", 15, "the sender's session description")
        if self._crypto is None:
            raise SenderError("Session description arrived without a usable secret_key.")
        log.info(
            "sender voice: mode %s codec %s dave v%s",
            session.get("mode"), session.get("video_codec"), session.get("dave_protocol_version"),
        )
        await self._voice_send(12, {
            "audio_ssrc": self.audio_ssrc,
            "video_ssrc": self.video_ssrc,
            "rtx_ssrc": self.rtx_ssrc,
            "streams": [{
                "type": "video", "rid": "100", "ssrc": self.video_ssrc,
                "active": True, "quality": 100, "rtx_ssrc": self.rtx_ssrc,
                "max_bitrate": 3500000, "max_framerate": 30,
                "max_resolution": {"type": "fixed", "width": 1280, "height": 720},
            }],
        })
        # Reference streamers signal speaking on the stream socket before media.
        await self._voice_send(5, {
            "speaking": 2,
            "ssrc": self.audio_ssrc,
            "delay": 0,
        })

    def _on_udp_packet(self, data: bytes) -> None:
        if len(data) < 4:
            return
        packet_type = data[1]
        if not 192 <= packet_type <= 223:  # not RTCP
            return
        self.rtcp_received += 1
        if self._udp_logged < 4:
            self._udp_logged += 1
            log.info("sender udp: RTCP type %d size %d", packet_type, len(data))
        # Walk the compound RTCP packet: a viewer PLI (PSFB, FMT 1) means the
        # viewer needs an IDR, which only A can produce.
        pos = 0
        while pos + 4 <= len(data):
            sub_type = data[pos + 1]
            length = int.from_bytes(data[pos + 2:pos + 4], "big")
            if 192 <= sub_type <= 223 and sub_type == 206 and (data[pos] & 0x1F) == 1:
                self.pli_requests += 1
                self._ask_for_keyframe()
            if length > 200:
                break
            pos += 4 * (length + 1)

    def _ask_for_keyframe(self) -> None:
        now = time.monotonic()
        if now - self._last_pli_at < 1.0:
            return
        self._last_pli_at = now
        if self.on_keyframe_request is not None:
            self.on_keyframe_request()

    async def _discover(self, ip: str, port: int, ssrc: int) -> tuple[str, int]:
        loop = asyncio.get_running_loop()
        udp = self._udp = _Udp(self._on_udp_packet)
        await loop.create_datagram_endpoint(lambda: udp, remote_addr=(ip, port))
        transport = udp.transport
        assert transport is not None
        packet = struct.pack(">HHI", 1, 70, ssrc) + b"\x00" * 66
        for _ in range(3):
            transport.sendto(packet)
            try:
                return await asyncio.wait_for(asyncio.shield(udp.discovery), 2)
            except asyncio.TimeoutError:
                continue
        raise SenderError("No reply to UDP IP discovery from the stream server.")

    async def _voice_send(self, op: int, d: Any) -> None:
        assert self._voice_ws is not None
        await self._voice_ws.send_str(json.dumps({"op": op, "d": d}))

    async def _voice_loop(self) -> None:
        assert self._voice_ws is not None
        try:
            async for message in self._voice_ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._voice_handle_json(json.loads(message.data))
                elif message.type == aiohttp.WSMsgType.BINARY and len(message.data) >= 3:
                    seq = struct.unpack(">H", message.data[:2])[0]
                    self._voice_seq = seq
                    await self._voice_handle_binary(message.data[2], message.data[3:])
        except Exception as e:
            log.warning("sender voice loop error: %s", type(e).__name__)
        finally:
            if not self._closing:
                code = self._voice_ws.close_code
                log.warning("sender voice closed (code %s)", code)
                # The stream voice socket is the media path: frames can no
                # longer reach the SFU, so the relay is pointless.
                self._signal_end(f"stream socket closed (code {code})")

    async def _voice_handle_json(self, payload: dict) -> None:
        op = payload.get("op")
        d = payload.get("d") or {}
        if payload.get("seq") is not None:
            self._voice_seq = payload["seq"]
        if op == 8:
            target = self.target
            assert target is not None
            self._voice_tasks.append(
                asyncio.create_task(self._voice_heartbeat(d["heartbeat_interval"]))
            )
            await self._voice_send(0, {
                "server_id": target.rtc_server_id,
                "channel_id": target.rtc_channel_id,
                "user_id": self.user_id,
                "session_id": self.voice_session_id,
                "token": target.token,
                "video": True,
                "streams": [{"type": "screen", "rid": "100", "quality": 100}],
                "max_dave_protocol_version": 1,
            })
        elif op == 2:
            self.audio_ssrc = int(d.get("ssrc") or 0)
            # Video off first, matching the reference streamer's READY sequence.
            await self._voice_send(12, {
                "audio_ssrc": self.audio_ssrc,
                "video_ssrc": 0,
                "rtx_ssrc": 0,
                "streams": [],
            })
            self._resolve("voice_ready", d)
        elif op == 4:
            await self._on_session_description(d)
            self._resolve("session_description", d)
        elif op == 11:
            if self._dave is not None:
                self._dave.add_recognized_users(d.get("user_ids") or [])
        elif op == 13:
            if self._dave is not None and d.get("user_id"):
                self._dave.remove_recognized_user(d["user_id"])
        elif op == 21 and self._dave is not None:
            await self._dave_actions(self._dave.prepare_transition(
                int(d.get("transition_id", 0)), int(d.get("protocol_version", 0))
            ))
        elif op == 22 and self._dave is not None:
            self._dave.execute_transition(int(d.get("transition_id", 0)))
            if self.can_send:
                # Media is allowed once our ratchet exists; signal it.
                await self._voice_send(5, {
                    "speaking": 2,
                    "ssrc": self.audio_ssrc,
                    "delay": 0,
                })
                self.state = "relaying"
                log.info("sender: ratchet ready; speaking signal sent")
        elif op == 24 and self._dave is not None:
            await self._dave_actions(self._dave.on_prepare_epoch(
                int(d.get("epoch", 0)), int(d.get("protocol_version", 0))
            ))

    async def _on_session_description(self, d: dict) -> None:
        mode = d.get("mode")
        secret_key = d.get("secret_key")
        if mode and secret_key:
            try:
                self._crypto = TransportCrypto(str(mode), bytes(secret_key))
            except ValueError as e:
                raise SenderError(f"Sender transport crypto: {e}") from e
        if self._dave is not None:
            await self._dave_actions(self._dave.on_session_description(d))

    async def _voice_handle_binary(self, op: int, payload: bytes) -> None:
        dave = self._dave
        if dave is None:
            return
        if op == 25:
            dave.on_external_sender(payload)
        elif op == 27:
            await self._dave_actions(dave.on_proposals(payload))
        elif op in (29, 30) and len(payload) >= 2:
            transition_id = int.from_bytes(payload[:2], "big")
            if op == 29:
                await self._dave_actions(dave.on_commit(transition_id, payload[2:]))
            else:
                await self._dave_actions(dave.on_welcome(transition_id, payload[2:]))
            if self.can_send:
                self.state = "relaying"

    async def _dave_actions(self, actions) -> None:
        assert self._voice_ws is not None
        for action in actions:
            if action.data is not None:
                await self._voice_send(action.op, action.data)
            elif action.binary is not None:
                await self._voice_ws.send_bytes(bytes([action.op]) + action.binary)

    async def _voice_heartbeat(self, interval_ms: int) -> None:
        interval = interval_ms / 1000
        await asyncio.sleep(interval * random.random())
        while True:
            assert self._voice_ws is not None
            await self._voice_ws.send_str(json.dumps(
                {"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": self._voice_seq}}
            ))
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------ media

    def _map_timestamp(self, kind: str, source_ts: int | None, step: int) -> int:
        """Continue A's RTP timeline on our own random start timestamp.

        Screen streams are variable frame rate: a fixed per-frame increment
        makes playback pace drift and latency grow. With A's timestamps the
        viewer sees the original cadence. A long gap or an encoder reset
        rebases the offset so one stale frame does not stall the timeline.
        """
        if kind == "audio":
            ts, offset, last = self._audio_ts, self._audio_ts_offset, self._audio_src_ts
        else:
            ts, offset, last = self._video_ts, self._video_ts_offset, self._video_src_ts
        if source_ts is None:
            ts = (ts + step) & 0xFFFFFFFF
        else:
            if last is not None:
                delta = (source_ts - last) & 0xFFFFFFFF
                if delta >= 0x80000000 or delta > step * 1800:  # backwards or ~minute gap
                    offset = None
            if offset is None:
                offset = (ts - source_ts) & 0xFFFFFFFF
            ts = (source_ts + offset) & 0xFFFFFFFF
            last = source_ts
        if kind == "audio":
            self._audio_ts, self._audio_ts_offset, self._audio_src_ts = ts, offset, last
        else:
            self._video_ts, self._video_ts_offset, self._video_src_ts = ts, offset, last
        return ts

    def _send_rtp(
        self,
        ssrc: int,
        payload_type: int,
        seq: int,
        timestamp: int,
        payload: bytes,
        *,
        marker: bool,
    ) -> None:
        if self._udp is None or self._udp.transport is None or self._crypto is None:
            return
        header = struct.pack(
            ">BBHII",
            0x80,
            (0x80 if marker else 0) | (payload_type & 0x7F),
            seq & 0xFFFF,
            timestamp & 0xFFFFFFFF,
            ssrc & 0xFFFFFFFF,
        )
        self._counter = (self._counter + 1) & 0xFFFFFFFF
        packet = self._crypto.encrypt_packet(header, payload, self._counter)
        self._udp.transport.sendto(packet)
        self.packets_sent += 1
        self.bytes_sent += len(packet)
