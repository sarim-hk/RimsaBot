"""Milestones 3a/3b: the Go Live stream's voice connection over *plain UDP*.

3a proved the stream server accepts the classic UDP voice protocol instead of
WebRTC (op1 Select Protocol `udp` -> op4, then RTP over UDP).

3b adds decryption, layer by layer (see utils/dave_media.py):

  voice WS: op8 Hello -> op0 Identify -> op2 Ready
  UDP:      IP discovery against Ready's ip/port
  voice WS: op1 Select Protocol (udp) -> op4 Session Description
  DAVE:     op4/op24 -> op26 key package; op25/27/29/30 join the MLS group;
            op21/op22 install the streamer's per-epoch key ratchet
  media:    UDP -> transport AEAD (secret_key) -> DAVE frame -> plaintext

Logging names and counts only, never tokens, keys or media.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import struct
import time
from pathlib import Path
from typing import Any, Callable

import aiohttp

from utils import dave_media
from utils.dave_media import (
    DaveAction,
    DaveState,
    OPUS_PAYLOAD_TYPE,
    TransportCrypto,
    frame_metadata,
    frame_nonce,
)
from utils.user_gateway import StreamInfo

log = logging.getLogger(__name__)

# The captured browser session used v=9 for the stream voice socket; v8+ share
# the seq_ack heartbeat and the binary DAVE ops. Match the capture.
VOICE_GATEWAY_VERSION = 9

VOICE_CLOSE_REASONS = {
    1006: "abnormal closure (no close frame)",
    4004: "authentication failed (token rejected)",
    4006: "session no longer valid",
    4008: "rate limited",
    4014: "disconnected (kicked)",
    4015: "voice server crashed",
    4016: "unknown protocol",
}
PREFERRED_MODES = ("aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize")

# Failing video frames are dumped here for offline reassembly analysis: the
# transport-decrypted RTP payloads, sequence order and per-packet flags.
FRAME_DUMP_DIR = "capture"
FRAME_DUMP_FAILURES = 5
FRAME_DUMP_SUCCESSES = 2
KEYFRAME_REQUEST_INTERVAL = 2.0  # seconds between self-heal PLIs
# Relay mode ends after this long without a single RTP packet from A: the
# stream voice socket does not always close when the stream stops.
MEDIA_IDLE_TIMEOUT = 15.0

# Binary DAVE opcodes. 21/22/24/23/31 travel as JSON, the rest as raw MLS.
DAVE_OPS = {
    21: "prepare_transition", 22: "execute_transition", 23: "transition_ready",
    24: "prepare_epoch", 25: "mls_external_sender", 26: "mls_key_package",
    27: "mls_proposals", 28: "mls_commit_welcome", 29: "mls_announce_commit",
    30: "mls_welcome", 31: "mls_invalid_commit_welcome",
}

OPUS_SILENCE = b"\xf8\xff\xfe"
# Payload types announced in Select Protocol. Other payload types on the same
# SSRC (Discord sends occasional PT 96 packets) are not H264/Opus fragments.
AUDIO_PT = 120
VIDEO_PT = 101
RTX_PT = 102


class ProbeError(Exception):
    pass


class _Udp(asyncio.DatagramProtocol):
    def __init__(self, on_media: Callable[[bytes], None] | None = None) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.discovery: asyncio.Future[tuple[str, int]] = (
            asyncio.get_running_loop().create_future()
        )
        self.on_media = on_media
        self.packets = 0
        self.bytes = 0
        self.rtcp = 0
        self.payload_types: dict[int, int] = {}

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        if not self.discovery.done() and len(data) >= 74 and data[:2] == b"\x00\x02":
            ip = data[8:72].split(b"\x00", 1)[0].decode()
            port = struct.unpack(">H", data[72:74])[0]
            self.discovery.set_result((ip, port))
            return
        if len(data) < 2 or data[0] >> 6 != 2:  # not RTP/RTCP
            return
        if 200 <= data[1] <= 204:  # RTCP is not transport-encrypted media
            self.rtcp += 1
            return
        self.packets += 1
        self.bytes += len(data)
        pt = data[1] & 0x7F
        self.payload_types[pt] = self.payload_types.get(pt, 0) + 1
        if self.on_media is not None:
            self.on_media(data)


class StreamProbe:
    def __init__(
        self,
        info: StreamInfo,
        on_frame: Callable[[str, bytes, int], None] | None = None,
        *,
        idle_timeout: float | None = MEDIA_IDLE_TIMEOUT,
    ) -> None:
        self._info = info
        self._on_frame = on_frame
        # In relay mode (listen=None) the probe gives up after this long with
        # no RTP from A; None (or 0) disables the watchdog.
        self.idle_timeout = idle_timeout
        self.idle_expired = False
        self._last_media_at = 0.0  # monotonic time of the last RTP packet
        self._http: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._udp: _Udp | None = None
        self._tasks: list[asyncio.Task] = []
        self._seq = -1
        self._ready: asyncio.Future | None = None
        self._session: asyncio.Future | None = None
        self.trace: list[str] = []
        self.dave_ops: dict[str, int] = {}
        self.status = "not started"
        self.close_code: int | None = None
        self._done = False
        self._aborted = False
        self._closed = False  # close() has been called at least once
        self._closed_event = asyncio.Event()

        # Decryption state (3b)
        self._crypto: TransportCrypto | None = None
        self._dave: DaveState | None = None
        self._audio_ssrcs: set[int] = set()
        self._video_ssrcs: set[int] = set()
        self._rtx_ssrcs: set[int] = set()
        self._rtx_map: dict[int, int] = {}  # rtx ssrc -> primary video ssrc
        self._transport_failures = 0
        self._ext_packets = 0
        self._media_seen = 0
        self._rtx_packets = 0
        self._ignored_packets = 0
        self._ignored_logged = 0
        self._undecryptable = 0  # media arrived before a ratchet existed
        self._decrypt_disabled: set[str] = set()  # kind -> bookkeeping gave up
        self._media_stats: dict[str, dict[str, int]] = {
            "audio": {"ok": 0, "fail": 0, "silence": 0, "bytes": 0},
            "video": {"ok": 0, "fail": 0, "silence": 0, "bytes": 0},
        }
        # Video arrives as RTP fragments; reassemble per SSRC/timestamp and
        # decrypt only complete frames (marker bit = last packet).
        self._video_buffers: dict[int, dict] = {}
        self._video_last_ts: dict[int, int] = {}
        self._video_next_seq: dict[int, int] = {}
        self._video_fragments = 0
        self._video_incomplete = 0
        self._video_codec: str | None = None
        # RTCP PLI keyframe requests: our own voice SSRC is the RTCP sender,
        # the streamer's video SSRCs are the media sources.
        self._self_ssrc: int | None = None
        self._rtcp_counter = random.getrandbits(32)
        self._pli_sent = 0
        self._last_keyframe_request = 0.0
        self._dump_failures = 0
        self._dump_successes = 0
        self._video_frag_logged = 0
        self._video_layout: int | None = None  # winning start-code layout
        self._video_probe_frames = 0
        self._video_alt_probes = 0
        self._video_meta_logged = 0
        self._video_fail_meta_logged = 0
        self._video_success_detail = 0
        self._rtx_logged = 0
        self._nonce_logged = {"audio": 0, "video": 0}
        self.MAX_VIDEO_FRAME_BYTES = 8 * 1024 * 1024
        if dave_media.available():
            try:
                # A Go Live media session has its own MLS group, whose
                # snowflake is rtc_server_id - 1 (not the rtc channel id).
                self._dave = DaveState(
                    group_id=int(info.rtc_server_id) - 1,
                    self_user_id=info.user_id,
                    streamer_user_id=info.streamer_user_id,
                )
            except Exception as e:
                log.warning("DAVE state unavailable: %s", type(e).__name__)
            else:
                log.info(
                    "stream voice: rtc server %s, rtc channel %s, dave group %s",
                    info.rtc_server_id,
                    info.rtc_channel_id,
                    int(info.rtc_server_id) - 1,
                )

    # ---------------------------------------------------------------- public

    async def run(self, *, timeout: float = 15.0, listen: float | None = 20.0) -> str:
        """Do the handshake, listen (forever if `listen` is None), return a summary."""
        loop = asyncio.get_running_loop()
        self._ready = loop.create_future()
        self._session = loop.create_future()
        try:
            self.status = "connecting to stream voice server"
            await self._connect()

            ready = await self._wait(self._ready, timeout, "Ready (op2)")
            mode = next((m for m in PREFERRED_MODES if m in ready.get("modes", [])), None)
            if mode is None:
                raise ProbeError(f"no supported encryption mode in {ready.get('modes')}")
            if ready.get("ssrc"):
                self._self_ssrc = int(ready["ssrc"])

            self.status = "UDP IP discovery"
            ip, port = await self._discover(ready["ip"], ready["port"], ready["ssrc"])
            self._note("udp discovery ok")

            self.status = "selecting UDP protocol"
            await self._send(1, {
                "protocol": "udp",
                "data": {"address": ip, "port": port, "mode": mode},
                "codecs": [
                    {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120},
                    {"name": "H264", "type": "video", "priority": 1000, "payload_type": 101,
                     "rtx_payload_type": 102, "encode": False, "decode": True},
                    {"name": "VP8", "type": "video", "priority": 900, "payload_type": 103,
                     "rtx_payload_type": 104, "encode": False, "decode": True},
                    {"name": "VP9", "type": "video", "priority": 800, "payload_type": 105,
                     "rtx_payload_type": 106, "encode": False, "decode": True},
                ],
            })
            sd = await self._wait(self._session, timeout, "Session Description (op4)")
            self.status = (
                f"session ok: mode={sd.get('mode')} video={sd.get('video_codec')} "
                f"audio={sd.get('audio_codec')} dave={sd.get('dave_protocol_version')}; listening"
            )
            self._note("got session description")

            if listen is None:
                await self._wait_for_end()
                if self.idle_expired:
                    self.status = (
                        f"aborted: no media for {self.idle_timeout:.0f}s. "
                        f"{self.summary()}"
                    )
                    return self.status
            else:
                await asyncio.sleep(listen)
            self.status = self.summary()
            return self.status
        except ProbeError as e:
            if self._aborted:
                self.status = f"aborted: watcher ended before the probe finished. {self.summary()}"
            else:
                self.status = f"failed: {e}. {self.summary()}"
            return self.status
        finally:
            self._done = True
            await self.close()

    def summary(self) -> str:
        udp = self._udp
        media = (
            f"udp {udp.packets} rtp / {udp.bytes // 1024} KiB, rtcp {udp.rtcp}, "
            f"rtx {self._rtx_packets}, pli {self._pli_sent}, "
            f"ignored {self._ignored_packets}, pt {udp.payload_types}"
            if udp else "udp not opened"
        )
        dave = ", ".join(f"{k}x{v}" for k, v in self.dave_ops.items()) or "none"
        if self._dave is None:
            dave_stats = "dave: unavailable"
        else:
            ds = self._dave
            a, v = self._media_stats["audio"], self._media_stats["video"]
            parts = [
                f"kp {ds.key_packages_sent}",
                f"welcome {ds.welcomes}",
                f"commit {ds.commits}",
                "ratchet yes" if ds.has_ratchet else "ratchet no",
                f"audio ok {a['ok']} (silence {a['silence']})/fail {a['fail']}",
                f"video ok {v['ok']}/fail {v['fail']} "
                f"(frags {self._video_fragments}, lost {self._video_incomplete})",
            ]
            dave_stats = "dave: " + ", ".join(parts)
            if ds.trace:
                dave_stats += " (" + ", ".join(ds.trace[-4:]) + ")"
        transport = "transport: " + (
            f"{self._crypto.mode}, failed {self._transport_failures}, "
            f"ext {self._ext_packets}"
            if self._crypto else "none"
        )
        tail = ", ".join(event[:60] for event in self.trace[-3:]) or "none"
        code = f"; ws closed {self.close_code}" if self.close_code else ""
        return (
            f"{media}; {transport}; {dave_stats}; waiting-for-ratchet "
            f"{self._undecryptable}; dave ops: {dave}{code}; last events: {tail}"
        )

    def stats(self) -> dict[str, Any]:
        """Friendly counters for `/restream status` (summary() is the raw dump)."""
        return {
            "running": not self._done and self.close_code is None,
            "ratchet": bool(self._dave is not None and self._dave.has_ratchet),
            "audio_ok": self._media_stats["audio"]["ok"],
            "audio_fail": self._media_stats["audio"]["fail"],
            "video_ok": self._media_stats["video"]["ok"],
            "video_fail": self._media_stats["video"]["fail"],
            "pli": self._pli_sent,
            "idle_expired": self.idle_expired,
        }

    def request_keyframe(self) -> bool:
        """Ask the stream's SFU for a fresh H264 keyframe (RTCP PLI).

        This is what official viewer clients send when they need an IDR: an
        RTCP PSFB with FMT 1 and the streamer's video SSRC. In the rtpsize
        transport modes it is encrypted exactly like RTP (the 8-byte RTCP
        header is the AAD, the 4-byte counter is appended).

        Safe to call at any time; returns False if the media path is not up.
        """
        if self._crypto is None or self._udp is None or self._udp.transport is None:
            return False
        if not self._self_ssrc or not self._video_ssrcs:
            return False
        sent = 0
        for ssrc in sorted(self._video_ssrcs):
            header = struct.pack(
                ">BBHI", 0x81, 206, 2, self._self_ssrc  # V=2, FMT 1, PSFB, 3 words
            )
            body = struct.pack(">I", ssrc)  # media source SSRC, empty FCI
            self._rtcp_counter = (self._rtcp_counter + 1) & 0xFFFFFFFF
            self._udp.transport.sendto(
                self._crypto.encrypt_packet(header, body, self._rtcp_counter)
            )
            sent += 1
        if sent:
            self._pli_sent += 1
            self._note(f"requested keyframe (PLI) for {sent} video ssrc(s)")
        return bool(sent)

    async def close(self) -> None:
        self._closed = True
        self._closed_event.set()
        if not self._done:
            self._aborted = True  # closed from outside while run() was still going
        current = asyncio.current_task()
        for t in self._tasks:
            if t is not current:
                t.cancel()
        for buffer in self._video_buffers.values():
            timer = buffer.get("timer")
            if timer is not None:
                timer.cancel()
        self._video_buffers.clear()
        if self._udp and self._udp.transport:
            self._udp.transport.close()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._http is not None and not self._http.closed:
            await self._http.close()

    # -------------------------------------------------------------- internals

    async def _wait_for_end(self) -> None:
        """Relay mode: block until the socket closes or media stops arriving.

        A stream can stop without its voice socket closing (a paused Go Live,
        a dead SFU session, a streamer that left), and when the watcher's
        gateway already dropped no STREAM_DELETE is seen. Without a watchdog
        the relay would stay "active" while forwarding nothing.
        """
        if not self.idle_timeout:
            await self._closed_event.wait()
            return
        self._last_media_at = time.monotonic()
        while not self._closed_event.is_set():
            remaining = self._last_media_at + self.idle_timeout - time.monotonic()
            if remaining <= 0:
                self.idle_expired = True
                self._note(f"no media for {self.idle_timeout:.0f}s; ending the relay")
                return
            try:
                await asyncio.wait_for(self._closed_event.wait(), remaining)
            except asyncio.TimeoutError:
                continue

    def _note(self, event: str) -> None:
        self.trace.append(event)
        log.info("stream voice: %s", event)

    async def _wait(self, fut: asyncio.Future, timeout: float, what: str) -> Any:
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            raise ProbeError(f"timed out waiting for {what}") from None

    async def _connect(self) -> None:
        host = self._info.endpoint.removeprefix("wss://").rstrip("/")
        url = f"wss://{host}/?v={VOICE_GATEWAY_VERSION}"
        self._http = aiohttp.ClientSession()
        if self._closed:  # close() raced us while the session was being created
            await self._http.close()
            raise ProbeError("probe was closed during startup")
        try:
            self._ws = await self._http.ws_connect(url, max_msg_size=0)
        except aiohttp.ClientError as e:
            raise ProbeError(f"couldn't reach stream voice server: {e}") from e
        self._tasks.append(asyncio.create_task(self._read_loop()))

    async def _send(self, op: int, d: Any) -> None:
        assert self._ws is not None
        self._note(f"sent op{op}")
        await self._ws.send_str(json.dumps({"op": op, "d": d}))

    async def _send_bin(self, op: int, payload: bytes) -> None:
        assert self._ws is not None
        self._note(f"sent binary op{op} ({len(payload)} B)")
        await self._ws.send_bytes(bytes([op]) + payload)

    async def _send_actions(self, actions: list[DaveAction]) -> None:
        for action in actions:
            if action.data is not None:
                await self._send(action.op, action.data)
            elif action.binary is not None:
                await self._send_bin(action.op, action.binary)

    async def _discover(self, ip: str, port: int, ssrc: int) -> tuple[str, int]:
        loop = asyncio.get_running_loop()
        udp = self._udp = _Udp(self._on_media)
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
        raise ProbeError("no reply to UDP IP discovery (UDP blocked?)")

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_json(json.loads(msg.data))
                elif msg.type == aiohttp.WSMsgType.BINARY and len(msg.data) >= 3:
                    self._seq = struct.unpack(">H", msg.data[:2])[0]
                    op = msg.data[2]
                    name = DAVE_OPS.get(op, f"op{op}")
                    self.dave_ops[name] = self.dave_ops.get(name, 0) + 1
                    await self._handle_binary(op, msg.data[3:])
        except Exception as e:
            self._note(f"read error {type(e).__name__}")
        finally:
            self._closed_event.set()
            self.close_code = self._ws.close_code
            code = self.close_code
            why = (
                VOICE_CLOSE_REASONS.get(code, "unknown reason")
                if code is not None
                else "unknown reason"
            )
            err = ProbeError(
                f"voice websocket closed (code {self.close_code}: {why})"
            )
            for fut in (self._ready, self._session):
                if fut is not None and not fut.done():
                    fut.set_exception(err)
                    fut.add_done_callback(lambda f: f.exception())

    async def _handle_json(self, p: dict) -> None:
        op, d = p["op"], p.get("d") or {}
        if p.get("seq") is not None:
            self._seq = p["seq"]
        self._note(f"got op{op}")
        if op == 8:  # Hello
            self._tasks.append(asyncio.create_task(self._heartbeat(d["heartbeat_interval"])))
            info = self._info
            await self._send(0, {
                "server_id": info.rtc_server_id,
                "channel_id": info.rtc_channel_id,
                "user_id": info.user_id,
                "session_id": info.voice_session_id,
                "token": info.token,
                "video": True,
                "streams": [{"type": "screen", "rid": "100", "quality": 100}],
                "max_dave_protocol_version": 1,
            })
        elif op == 2 and self._ready and not self._ready.done():
            self._ready.set_result(d)
        elif op == 4 and self._session and not self._session.done():
            await self._on_session_description(d)  # contains secret_key: never log d
            self._session.set_result(d)
        elif op == 11:  # clients connect: the stream's voice roster
            if self._dave is not None and isinstance(d, dict):
                self._dave.add_recognized_users(d.get("user_ids") or [])
        elif op == 12:  # SSRC definitions (audio + simulcast video layers)
            if isinstance(d, dict):
                self._note_ssrcs(d)
        elif op == 13:  # client disconnect
            if self._dave is not None and isinstance(d, dict) and d.get("user_id"):
                self._dave.remove_recognized_user(d["user_id"])
        elif op == 21 and self._dave is not None and isinstance(d, dict):
            await self._send_actions(self._dave.prepare_transition(
                int(d.get("transition_id", 0)), int(d.get("protocol_version", 0))
            ))
        elif op == 22 and self._dave is not None and isinstance(d, dict):
            self._dave.execute_transition(int(d.get("transition_id", 0)))
        elif op == 24 and self._dave is not None and isinstance(d, dict):
            await self._send_actions(self._dave.on_prepare_epoch(
                int(d.get("epoch", 0)), int(d.get("protocol_version", 0))
            ))

    async def _on_session_description(self, d: dict) -> None:
        mode = d.get("mode")
        secret_key = d.get("secret_key")
        self._video_codec = d.get("video_codec")
        self._note(f"codecs: video {self._video_codec}, audio {d.get('audio_codec')}")
        if mode and secret_key:
            try:
                self._crypto = TransportCrypto(str(mode), bytes(secret_key))
            except ValueError as e:
                self._note(f"transport crypto: {e}")
        elif mode in PREFERRED_MODES:
            self._note("session description had no secret_key; media can't be peeled")
        if self._dave is not None:
            await self._send_actions(self._dave.on_session_description(d))

    async def _handle_binary(self, op: int, payload: bytes) -> None:
        dave = self._dave
        if dave is None:
            return
        if op == 25:  # external sender package
            dave.on_external_sender(payload)
        elif op == 27:  # proposals
            await self._send_actions(dave.on_proposals(payload))
        elif op in (29, 30):  # commit/welcome + 2-byte transition id
            if len(payload) < 2:
                self._note(f"short op{op} frame")
                return
            transition_id = int.from_bytes(payload[:2], "big")
            if op == 29:
                await self._send_actions(dave.on_commit(transition_id, payload[2:]))
            else:
                await self._send_actions(dave.on_welcome(transition_id, payload[2:]))

    # ------------------------------------------------------------ media path

    def _note_ssrcs(self, d: dict) -> None:
        if d.get("audio_ssrc"):
            self._audio_ssrcs.add(int(d["audio_ssrc"]))
        video = d.get("video_ssrc")
        rtx = d.get("rtx_ssrc")
        if video:
            self._video_ssrcs.add(int(video))
        if rtx:
            self._rtx_ssrcs.add(int(rtx))
            if video:
                self._rtx_map[int(rtx)] = int(video)
        for stream in d.get("streams") or []:
            if not isinstance(stream, dict):
                continue
            primary = stream.get("ssrc")
            retransmit = stream.get("rtx_ssrc")
            if primary:
                self._video_ssrcs.add(int(primary))
            if retransmit:
                self._rtx_ssrcs.add(int(retransmit))
                if primary:
                    self._rtx_map[int(retransmit)] = int(primary)
        self._note(
            f"ssrcs known: {len(self._audio_ssrcs)} audio, "
            f"{len(self._video_ssrcs)} video, {len(self._rtx_ssrcs)} rtx "
            f"({len(self._rtx_map)} mapped)"
        )

    def _media_kind(self, packet: bytes) -> str:
        ssrc = struct.unpack(">I", packet[8:12])[0]
        if ssrc in self._video_ssrcs:
            return "video"
        if ssrc in self._audio_ssrcs:
            return "audio"
        return "audio" if (packet[1] & 0x7F) == OPUS_PAYLOAD_TYPE else "video"

    def _on_media(self, packet: bytes) -> None:
        if len(packet) < 12:
            return
        # Any RTP from A means the source is still live, even before our
        # transport/DAVE keys are ready (undecryptable frames still count).
        self._last_media_at = time.monotonic()
        ssrc = int.from_bytes(packet[8:12], "big")
        pt = packet[1] & 0x7F
        is_rtx = ssrc in self._rtx_ssrcs
        if is_rtx:
            self._rtx_packets += 1
            if ssrc not in self._rtx_map or pt != RTX_PT:
                self._ignored_packets += 1
                return
        if self._crypto is None:
            self._undecryptable += 1  # transport layer still wrapped
            return
        result = self._crypto.decrypt_packet(packet)
        if result is None:
            self._transport_failures += 1
            return
        _, payload, extension_bytes = result
        if not payload:
            # Padding-only / empty packets are not media; if one landed inside
            # a frame's sequence range it must not count as a fragment.
            self._ignored_packets += 1
            return
        self._media_seen += 1
        if extension_bytes:
            self._ext_packets += 1
        if self._media_seen <= 6:
            self._note(
                f"media #{self._media_seen}: ssrc {ssrc} pt {packet[1] & 0x7F} "
                f"cc {packet[0] & 0x0F} ext {extension_bytes} "
                f"pad {int(bool(packet[0] & 0x20))} "
                f"payload {len(payload)} marker {bool(packet[1] & 0x80)} "
                f"rtx {int(is_rtx)}"
            )
        if self._dave is None or not self._dave.has_ratchet:
            self._undecryptable += 1  # waiting for the MLS group
            return
        timestamp = int.from_bytes(packet[4:8], "big")
        marker = bool(packet[1] & 0x80)
        if is_rtx:
            # RFC 4588: timestamp and marker are the original's; payload is
            # the original sequence number followed by the original payload.
            if len(payload) < 3:
                return
            original_seq = int.from_bytes(payload[:2], "big")
            if self._rtx_logged < 4:
                self._rtx_logged += 1
                self._note(
                    f"rtx frag: {ssrc}->{self._rtx_map[ssrc]} ts {timestamp} "
                    f"osn {original_seq} marker {int(marker)} bytes {len(payload) - 2}"
                )
            self._on_video_fragment(
                self._rtx_map[ssrc], timestamp, original_seq, payload[2:], marker,
                via_rtx=True, extension_bytes=extension_bytes, header=packet[:16],
            )
            return
        kind = self._media_kind(packet)
        expected_pt = VIDEO_PT if kind == "video" else AUDIO_PT
        if pt != expected_pt:
            # Not a fragment of this kind (Discord sends PT 96 packets on the
            # video SSRC). Feeding them to the reassembler corrupts frames.
            self._ignored_packets += 1
            if self._ignored_logged < 4:
                self._ignored_logged += 1
                self._note(
                    f"ignored media: ssrc {ssrc} pt {pt} "
                    f"(expected {expected_pt}, {len(payload)} B)"
                )
            return
        if kind == "video":
            seq = int.from_bytes(packet[2:4], "big")
            self._on_video_fragment(
                ssrc, timestamp, seq, payload, marker,
                extension_bytes=extension_bytes, header=packet[:16],
            )
        else:
            self._decrypt_frame("audio", payload, timestamp)

    def _video_nals(self, payload: bytes) -> list[tuple[bool, bytes]]:
        """Split an H264 RTP payload into NAL units.

        Returns (starts_new_nal, bytes) items. FU-A continuation fragments
        append to the previous NAL; STAP-A yields several NALs; single NAL
        units pass through. Packetization headers never reach DAVE.
        """
        if not payload:
            return []
        if self._video_codec not in (None, "H264"):
            return [(True, payload)]
        nal_type = payload[0] & 0x1F
        if nal_type == 28:  # FU-A
            if len(payload) < 3:
                return []
            fu_header = payload[1]
            if fu_header & 0x80:  # start: rebuild the original NAL header
                header = (payload[0] & 0xE0) | (fu_header & 0x1F)
                return [(True, bytes([header]) + payload[2:])]
            return [(False, payload[2:])]
        if nal_type == 24:  # STAP-A
            items: list[tuple[bool, bytes]] = []
            i = 1
            while i + 2 <= len(payload):
                size = int.from_bytes(payload[i:i + 2], "big")
                i += 2
                if size <= 0 or i + size > len(payload):
                    break
                items.append((True, payload[i:i + size]))
                i += size
            return items
        return [(True, payload)]  # single NAL unit

    VIDEO_REASSEMBLY_GRACE = 0.04  # seconds to wait for RTX after a marker

    def _on_video_fragment(
        self, ssrc: int, timestamp: int, seq: int, payload: bytes, marker: bool,
        via_rtx: bool = False, extension_bytes: int = 0, header: bytes = b"",
    ) -> None:
        """Collect one video fragment (primary or RTX) into its frame."""
        buffer = self._video_buffers.get(ssrc)
        if buffer is not None and buffer["ts"] != timestamp:
            self._finalize_video_frame(ssrc)
            buffer = None
        if buffer is None:
            last = self._video_last_ts.get(ssrc)
            if last is not None:
                # Late retransmit for a frame already handed to DAVE.
                if (last - timestamp) & 0xFFFFFFFF < 0x80000000:
                    return
            # If this is not the sequence number expected after the previous
            # frame, the frame's first packets are missing. Anchor the frame
            # at the expected start so the gap is visible, RTX can fill it,
            # and a still-incomplete frame is dropped instead of decrypted.
            expected = self._video_next_seq.get(ssrc)
            start_gap = 0
            first = seq
            if expected is not None:
                distance = (seq - expected) & 0xFFFF
                if 0 < distance < 0x8000:
                    start_gap = distance
                    first = expected
            buffer = self._video_buffers[ssrc] = {
                "ts": timestamp,
                "packets": {},
                "headers": {},
                "rtx": set(),
                "flags": {},
                "first_seq": first,
                "marker_seq": None,
                "timer": None,
                "size": 0,
                "start_gap": start_gap,
            }
        if seq in buffer["packets"]:
            return  # duplicate: primary vs RTX or repeated retransmit
        buffer["packets"][seq] = payload
        if header:
            buffer["headers"][seq] = header
        if via_rtx:
            buffer["rtx"].add(seq)
        nal = payload[0] & 0x1F if payload else -1
        fu = ""
        if nal == 28 and len(payload) > 1:
            fu = ("S" if payload[1] & 0x80 else "s")
            fu += ("E" if payload[1] & 0x40 else "e")
        buffer["flags"][seq] = f"{nal}{fu}/x{extension_bytes}"
        buffer["size"] += len(payload)
        self._video_fragments += 1
        if self._video_frag_logged < 8 and payload:
            self._video_frag_logged += 1
            self._note(
                f"video frag: ssrc {ssrc} seq {seq} nal {payload[0] & 0x1F} "
                f"bytes {len(payload)} marker {int(marker)}"
            )
        if marker:
            buffer["marker_seq"] = seq
            if self._video_frame_complete(buffer):
                self._finalize_video_frame(ssrc)
                return
            # Wait briefly for missing fragments to arrive via RTX.
            if buffer["timer"] is not None:
                buffer["timer"].cancel()
            buffer["timer"] = asyncio.get_running_loop().call_later(
                self.VIDEO_REASSEMBLY_GRACE, self._finalize_scheduled, ssrc
            )
        if buffer["size"] > self.MAX_VIDEO_FRAME_BYTES:
            if buffer["timer"] is not None:
                buffer["timer"].cancel()
            self._video_buffers.pop(ssrc, None)
            self._video_incomplete += 1

    @staticmethod
    def _video_frame_range(buffer: dict) -> tuple[int, int] | None:
        """(first_seq, packet_count) implied by the marker, or None."""
        marker = buffer["marker_seq"]
        if marker is None:
            return None
        first = buffer["first_seq"]
        return first, ((marker - first) & 0xFFFF) + 1

    def _video_frame_complete(self, buffer: dict) -> bool:
        frame_range = self._video_frame_range(buffer)
        if frame_range is None:
            return False
        first, count = frame_range
        present = sum(
            1 for seq in buffer["packets"] if ((seq - first) & 0xFFFF) < count
        )
        return present >= count

    def _finalize_scheduled(self, ssrc: int) -> None:
        self._finalize_video_frame(ssrc)

    def _finalize_video_frame(self, ssrc: int) -> None:
        buffer = self._video_buffers.pop(ssrc, None)
        if buffer is None:
            return
        timer = buffer.get("timer")
        if timer is not None:
            timer.cancel()
        self._video_last_ts[ssrc] = buffer["ts"]
        if buffer["marker_seq"] is not None:
            self._video_next_seq[ssrc] = (buffer["marker_seq"] + 1) & 0xFFFF
        packets = buffer["packets"]
        if not packets:
            self._video_incomplete += 1
            return
        first = buffer["first_seq"]
        frame_range = self._video_frame_range(buffer)
        if frame_range is not None:
            _, count = frame_range
            order = [seq for seq in packets if ((seq - first) & 0xFFFF) < count]
            if len(order) < count:
                # Still missing a fragment after the grace window: drop it
                # rather than feed a corrupt frame to DAVE.
                self._video_incomplete += 1
                return
        else:
            order = list(packets)
        order.sort(key=lambda seq: (seq - first) & 0xFFFF)
        if buffer["marker_seq"] is None and order:
            self._video_next_seq[ssrc] = (order[-1] + 1) & 0xFFFF
        nals: list[bytearray] = []
        first_starts: bool | None = None
        for seq in order:
            for starts_new, data in self._video_nals(packets[seq]):
                if first_starts is None:
                    first_starts = starts_new
                if starts_new:
                    nals.append(bytearray(data))
                elif nals:
                    nals[-1].extend(data)
        detail = ",".join(
            f"{seq}{'r' if seq in buffer['rtx'] else ''}:{len(packets[seq])}"
            f":{buffer['flags'].get(seq, '')}"
            for seq in order[:10]
        )
        info = (
            f"ts {buffer['ts']} seq {order[0]}..{order[-1]} "
            f"first_start {first_starts} marker {buffer['marker_seq']} "
            f"start_gap {buffer.get('start_gap', 0)} "
            f"pkts {len(packets)} rtx {len(buffer['rtx'])} [{detail}]"
        )
        self._decrypt_video_nals(
            [bytes(nal) for nal in nals if nal],
            info,
            buffer["ts"],
            {
                "ts": buffer["ts"],
                "order": order,
                "packets": packets,
                "headers": buffer.get("headers", {}),
                "flags": buffer["flags"],
                "rtx": sorted(buffer["rtx"]),
            },
        )

    def _decrypt_video_nals(
        self,
        nals: list[bytes],
        info: str = "",
        timestamp: int = 0,
        context: dict | None = None,
    ) -> None:
        dave = self._dave
        if dave is None:
            return
        if not nals:
            self._video_incomplete += 1
            return
        # The receive-side E2EE frame is Annex-B with 4-byte start codes in
        # the unencrypted ranges (spec: 3-byte codes are expanded to 4). The
        # depacketizer re-inserts them; without them the authenticated bytes
        # differ and every tag fails. The layout can vary by frame, so the
        # locked layout is tried first and the others only when it fails.
        sc4 = b"\x00\x00\x00\x01"
        sc3 = b"\x00\x00\x01"
        candidates = [
            b"".join(sc4 + nal for nal in nals),
            sc4.join(nals),
            b"".join(sc3 + nal for nal in nals),
            sc3.join(nals),
            b"".join(nals),
        ]
        if self._video_layout is not None:
            locked = self._video_layout
            attempts = [locked]
            if self._video_alt_probes < 40:
                attempts += [i for i in range(len(candidates)) if i != locked]
                self._video_alt_probes += 1
        elif self._video_probe_frames >= 40:
            attempts = [0]  # stop spamming libdave after 40 frames
            self._video_probe_frames += 1
        else:
            attempts = list(range(len(candidates)))
            self._video_probe_frames += 1
        if self._nonce_logged["video"] < 3:
            nonce = frame_nonce(candidates[0])
            if nonce is not None:
                self._nonce_logged["video"] += 1
                self._note(
                    f"video frame nonce {nonce} gen {nonce >> 24} "
                    f"size {len(candidates[0])} nals {len(nals)}"
                )
        if self._video_meta_logged < 4:
            self._video_meta_logged += 1
            nonce, ranges = frame_metadata(candidates[0])
            types = [(nal[0] & 0x1F, len(nal)) for nal in nals]
            self._note(
                f"video frame meta: nonce {nonce} size {len(candidates[0])} "
                f"nals {types} ranges {ranges[:12]} "
                f"first16 {candidates[0][:16].hex()}"
            )
        stats = self._media_stats["video"]
        if "video" in self._decrypt_disabled:
            stats["fail"] += 1
            return
        for index in attempts:
            frame = candidates[index]
            plaintext = dave.decrypt("video", frame)
            if plaintext is None:
                continue
            if self._video_layout != index:
                self._note(
                    f"video layout {index} accepted ({len(frame)} B, "
                    f"{len(nals)} nals) {info}"
                )
                self._video_layout = index
            if (
                self._video_success_detail < 3
                and nals and (nals[0][0] & 0x1F) == 1 and len(frame) > 1000
            ):
                self._video_success_detail += 1
                self._note(f"video ok detail: size {len(frame)} {info}")
            stats["ok"] += 1
            stats["bytes"] += len(plaintext)
            if self._dump_successes < FRAME_DUMP_SUCCESSES:
                self._dump_frame("ok", nals, info, context)
            if self._on_frame is not None:
                self._on_frame("video", plaintext, timestamp)
            return
        stats["fail"] += 1
        if self._dump_failures < FRAME_DUMP_FAILURES:
            self._dump_frame("fail", nals, info, context)
        self._maybe_request_keyframe()
        if self._video_fail_meta_logged < 4:
            self._video_fail_meta_logged += 1
            nonce, ranges = frame_metadata(candidates[0])
            types = [(nal[0] & 0x1F, len(nal)) for nal in nals]
            self._note(
                f"video fail meta: nonce {nonce} size {len(candidates[0])} "
                f"nals {types} ranges {ranges[:12]} "
                f"first16 {candidates[0][:16].hex()} {info}"
            )
        if stats["fail"] % 25 == 1:
            self._note(
                f"video fail: size {len(candidates[0])} nals {len(nals)} "
                f"layout {self._video_layout} {info}"
            )
        if stats["fail"] >= 50 and not stats["ok"]:
            self._decrypt_disabled.add("video")
            self._note("giving up on video: 50 straight decrypt failures")

    def _maybe_request_keyframe(self) -> None:
        """Self-heal: a dropped video frame stays visible until the next IDR."""
        now = time.monotonic()
        if now - self._last_keyframe_request < KEYFRAME_REQUEST_INTERVAL:
            return
        self._last_keyframe_request = now
        self.request_keyframe()

    def _dump_frame(
        self, label: str, nals: list[bytes], info: str, context: dict | None
    ) -> None:
        """Write one frame's transport-decrypted payloads for offline analysis."""
        if context is None:
            return
        index = self._dump_failures if label == "fail" else self._dump_successes
        path = Path(FRAME_DUMP_DIR) / f"{label}_{index:02d}.json"
        payloads = {}
        for seq in context["order"]:
            payload = context["packets"].get(seq)
            if payload is not None:
                payloads[str(seq)] = base64.b64encode(payload).decode()
        document = {
            "info": info,
            "ts": context["ts"],
            "order": context["order"],
            "flags": {str(k): v for k, v in context["flags"].items()},
            "headers": {
                str(seq): header.hex()
                for seq, header in context.get("headers", {}).items()
            },
            "rtx": context["rtx"],
            "nals": [[nal[0] & 0x1F, len(nal)] for nal in nals],
            "payloads": payloads,
        }
        try:
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(document), encoding="utf-8")
        except OSError as e:
            log.warning("could not dump frame: %s", e)
            return
        if label == "fail":
            self._dump_failures += 1
        else:
            self._dump_successes += 1
        self._note(f"dumped {label} video frame to {path}")

    def _decrypt_frame(self, kind: str, payload: bytes, timestamp: int = 0) -> None:
        dave = self._dave
        if dave is None:
            return
        stats = self._media_stats[kind]
        if kind in self._decrypt_disabled:
            stats["fail"] += 1
            return
        if self._nonce_logged[kind] < 3:
            nonce = frame_nonce(payload)
            if nonce is not None:
                self._nonce_logged[kind] += 1
                self._note(
                    f"{kind} frame nonce {nonce} gen {nonce >> 24} "
                    f"size {len(payload)}"
                )
        plaintext = dave.decrypt(kind, payload)
        if plaintext is None:
            stats["fail"] += 1
            if stats["fail"] >= 50 and not stats["ok"]:
                # libdave prints to stderr on every failure; don't flood the log
                self._decrypt_disabled.add(kind)
                self._note(f"giving up on {kind}: 50 straight decrypt failures")
            return
        stats["ok"] += 1
        stats["bytes"] += len(plaintext)
        if kind == "audio" and plaintext == OPUS_SILENCE:
            stats["silence"] += 1
        if self._on_frame is not None:
            self._on_frame(kind, plaintext, timestamp)

    async def _heartbeat(self, interval_ms: float) -> None:
        interval = interval_ms / 1000
        await asyncio.sleep(interval * random.random())
        while True:
            assert self._ws is not None
            await self._ws.send_str(json.dumps(
                {"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": self._seq}}
            ))
            await asyncio.sleep(interval)
