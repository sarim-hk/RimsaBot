from __future__ import annotations
import asyncio
import json
import logging
import random
import struct
import time
from typing import Any, Callable
import aiohttp
from utils import dave_media
from utils.dave_media import DaveAction, DaveState, TransportCrypto
from utils.stream_voice import (
    AUDIO_PT,
    DAVE_OPS,
    OPUS_SILENCE,
    PREFERRED_MODES,
    VOICE_CLOSE_REASONS,
    VOICE_GATEWAY_VERSION,
)

log = logging.getLogger(__name__)

# Receives the call audio from channel A: one voice socket per channel, DAVE
# decrypted per speaker (each speaker has their own ratchet).

class VoiceProbeError(Exception):
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
        if 200 <= data[1] <= 204:  # RTCP is not media
            self.rtcp += 1
            return
        self.packets += 1
        self.bytes += len(data)
        if self.on_media is not None:
            self.on_media(data)

class VoiceProbe:
    def __init__(
        self,
        *,
        guild_id: int,
        channel_id: int,
        user_id: str,
        session_id: str,
        endpoint: str,
        token: str,
        on_frame: Callable[[str, bytes, int], None] | None = None,
    ) -> None:
        self._guild_id = str(guild_id)
        self._channel_id = str(channel_id)
        self._user_id = str(user_id)
        self._session_id = session_id
        self._endpoint = endpoint
        self._token = token
        self._on_frame = on_frame

        self._http: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._udp: _Udp | None = None
        self._tasks: list[asyncio.Task] = []
        self._seq = -1
        self._ready: asyncio.Future | None = None
        self._session: asyncio.Future | None = None
        self._closed_event = asyncio.Event()
        self._closed = False
        self._done = False

        self._crypto: TransportCrypto | None = None
        self._dave: DaveState | None = None
        # dave_protocol_version 0 means the channel's media is not E2EE (the
        # transport layer still is), so frames are used as-is.
        self._dave_disabled = False
        if dave_media.available():
            try:
                # A voice channel's MLS group id is the channel snowflake.
                self._dave = DaveState(
                    group_id=int(channel_id),
                    self_user_id=self._user_id,
                    per_user=True,
                )
            except Exception as e:
                log.warning("voice probe: DAVE state unavailable: %s", type(e).__name__)

        self._ssrc_users: dict[int, str] = {}
        self._user_frames: dict[str, int] = {}
        self._frames_ok = 0
        self._pending = 0        # decrypted only once the speaker's ratchet exists
        self._unmapped = 0       # media before its op5 mapping arrived
        self._ignored = 0
        self._undecryptable = 0  # transport still wrapped
        self._silence = 0
        self._transport_failures = 0
        self._speaking_missing = 0
        self._unmapped_logged = 0

        self.trace: list[str] = []
        self.dave_ops: dict[str, int] = {}
        self.status = "not started"
        self.close_code: int | None = None

    async def run(self) -> str:
        loop = asyncio.get_running_loop()
        self._ready = loop.create_future()
        self._session = loop.create_future()
        try:
            self.status = "connecting to the voice server"
            await self._connect()
            ready = await self._wait(self._ready, 15, "Ready (op2)")
            mode = next((m for m in PREFERRED_MODES if m in ready.get("modes", [])), None)
            if mode is None:
                raise VoiceProbeError(f"no supported encryption mode in {ready.get('modes')}")

            self.status = "UDP IP discovery"
            ip, port = await self._discover(ready["ip"], ready["port"], ready["ssrc"])
            self._note("udp discovery ok")

            self.status = "selecting UDP protocol"
            await self._send(1, {
                "protocol": "udp",
                "data": {"address": ip, "port": port, "mode": mode},
                "codecs": [
                    {"name": "opus", "type": "audio", "priority": 1000,
                     "payload_type": AUDIO_PT},
                ],
            })
            sd = await self._wait(self._session, 15, "Session Description (op4)")
            self.status = f"listening (dave {sd.get('dave_protocol_version')})"
            self._note("started receiving call audio")
            await self._closed_event.wait()
            self.status = self.summary()
            return self.status
        except VoiceProbeError as e:
            self.status = f"failed: {e}. {self.summary()}"
            return self.status
        finally:
            self._done = True
            await self.close()

    async def close(self) -> None:
        self._closed = True
        self._closed_event.set()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current:
                task.cancel()
        if self._udp is not None and self._udp.transport is not None:
            self._udp.transport.close()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._http is not None and not self._http.closed:
            await self._http.close()

    def summary(self) -> str:
        udp = self._udp
        media = (
            f"udp {udp.packets} rtp / {udp.bytes // 1024} KiB, rtcp {udp.rtcp}"
            if udp else "udp not opened"
        )
        if self._dave is None:
            dave_stats = "dave: unavailable"
        else:
            ds = self._dave
            dave_stats = (
                f"dave: kp {ds.key_packages_sent}, welcome {ds.welcomes}, "
                f"commit {ds.commits}, ratchets {ds.voice_ratchets}, "
                f"ratchet {'yes' if ds.has_ratchet else 'no'}"
            )
        transport = "transport: " + (
            f"{self._crypto.mode}, failed {self._transport_failures}"
            if self._crypto else "none"
        )
        dave = ", ".join(f"{k}x{v}" for k, v in self.dave_ops.items()) or "none"
        code = f"; ws closed {self.close_code}" if self.close_code else ""
        return (
            f"{media}; users {len(self._user_frames)}, audio ok {self._frames_ok} "
            f"(silence {self._silence})/pending {self._pending}/unmapped {self._unmapped}, "
            f"ignored {self._ignored}; {transport}; {dave_stats}; "
            f"waiting-for-ratchet {self._undecryptable}; dave ops: {dave}{code}"
        )

    def stats(self) -> dict[str, Any]:
        return {
            "running": not self._done and not self._closed,
            "speakers": len(self._user_frames),
            "frames_ok": self._frames_ok,
            "frames_pending": self._pending,
            "ratchets": self._dave.voice_ratchets if self._dave is not None else 0,
        }

    def _note(self, event: str) -> None:
        self.trace.append(event)
        log.info("voice probe: %s", event)

    async def _wait(self, fut: asyncio.Future, timeout: float, what: str) -> Any:
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            raise VoiceProbeError(f"timed out waiting for {what}") from None

    async def _connect(self) -> None:
        host = self._endpoint.removeprefix("wss://").rstrip("/")
        url = f"wss://{host}/?v={VOICE_GATEWAY_VERSION}"
        self._http = aiohttp.ClientSession()
        if self._closed:  # the relay stopped while the session was being created
            await self._http.close()
            raise VoiceProbeError("probe was closed during startup")
        try:
            self._ws = await self._http.ws_connect(url, max_msg_size=0)
        except aiohttp.ClientError as e:
            raise VoiceProbeError(f"couldn't reach the voice server: {e}") from e
        self._tasks.append(asyncio.create_task(self._read_loop()))

    async def _send(self, op: int, d: Any) -> None:
        assert self._ws is not None
        await self._ws.send_str(json.dumps({"op": op, "d": d}))

    async def _send_bin(self, op: int, payload: bytes) -> None:
        assert self._ws is not None
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
        raise VoiceProbeError("no reply to UDP IP discovery (UDP blocked?)")

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
            code = self.close_code = self._ws.close_code
            why = VOICE_CLOSE_REASONS.get(code, "unknown reason")
            err = VoiceProbeError(f"voice websocket closed (code {code}: {why})")
            for fut in (self._ready, self._session):
                if fut is not None and not fut.done():
                    fut.set_exception(err)
                    fut.add_done_callback(lambda f: f.exception())

    async def _handle_json(self, p: dict) -> None:
        op, d = p["op"], p.get("d") or {}
        if p.get("seq") is not None:
            self._seq = p["seq"]
        if op == 8:  # Hello
            self._tasks.append(asyncio.create_task(self._heartbeat(d["heartbeat_interval"])))
            await self._send(0, {
                "server_id": self._guild_id,
                "user_id": self._user_id,
                "session_id": self._session_id,
                "token": self._token,
                "video": False,
                "streams": [],
                "max_dave_protocol_version": 1,
            })
        elif op == 2 and self._ready is not None and not self._ready.done():
            self._ready.set_result(d)
        elif op == 4 and self._session is not None and not self._session.done():
            await self._on_session_description(d)  # contains secret_key: never log d
            self._session.set_result(d)
        elif op == 5:  # Speaking: ssrc -> user id for every other speaker
            self._on_speaking(d)
        elif op == 11:  # clients connect: the channel's user roster
            if self._dave is not None and isinstance(d, dict):
                self._dave.add_recognized_users(d.get("user_ids") or [])
        elif op == 13:  # client disconnect
            if isinstance(d, dict) and d.get("user_id"):
                self._forget_user(str(d["user_id"]))
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

    def _forget_user(self, user_id: str) -> None:
        if self._dave is not None:
            self._dave.remove_recognized_user(user_id)
        for ssrc, mapped in list(self._ssrc_users.items()):
            if mapped == user_id:
                del self._ssrc_users[ssrc]

    def _on_speaking(self, d: dict) -> None:
        ssrc = d.get("ssrc")
        user_id = d.get("user_id")
        if ssrc is None or user_id is None:
            if self._speaking_missing < 3:
                self._speaking_missing += 1
                self._note(f"speaking event without ssrc/user_id: {sorted(d)}")
            return
        ssrc = int(ssrc)
        known = self._ssrc_users.get(ssrc)
        self._ssrc_users[ssrc] = str(user_id)
        if known != str(user_id) and len(self._ssrc_users) <= 8:
            self._note(f"ssrc {ssrc} -> user {user_id}")

    async def _on_session_description(self, d: dict) -> None:
        mode = d.get("mode")
        secret_key = d.get("secret_key")
        if mode and secret_key:
            try:
                self._crypto = TransportCrypto(str(mode), bytes(secret_key))
            except ValueError as e:
                self._note(f"transport crypto: {e}")
        elif mode in PREFERRED_MODES:
            self._note("session description had no secret_key; media can't be peeled")
        self._dave_disabled = int(d.get("dave_protocol_version") or 0) <= 0
        if self._dave_disabled:
            self._note("DAVE is off for this channel; media is transport-only")
        if self._dave is not None:
            await self._send_actions(self._dave.on_session_description(d))

    async def _handle_binary(self, op: int, payload: bytes) -> None:
        dave = self._dave
        if dave is None:
            return
        if op == 25:
            dave.on_external_sender(payload)
        elif op == 27:
            await self._send_actions(dave.on_proposals(payload))
        elif op in (29, 30):
            if len(payload) < 2:
                self._note(f"short op{op} frame")
                return
            transition_id = int.from_bytes(payload[:2], "big")
            if op == 29:
                await self._send_actions(dave.on_commit(transition_id, payload[2:]))
            else:
                await self._send_actions(dave.on_welcome(transition_id, payload[2:]))

    def _on_media(self, packet: bytes) -> None:
        if len(packet) < 12:
            return
        ssrc = int.from_bytes(packet[8:12], "big")
        pt = packet[1] & 0x7F
        if pt != AUDIO_PT:
            # Voice channels also carry video (cameras) on other payload
            # types; only Opus audio is relayed.
            self._ignored += 1
            return
        if self._crypto is None:
            self._undecryptable += 1
            return
        result = self._crypto.decrypt_packet(packet)
        if result is None:
            self._transport_failures += 1
            return
        _, payload, _ = result
        if not payload:
            self._ignored += 1
            return
        user_id = self._ssrc_users.get(ssrc)
        if user_id is None:
            self._unmapped += 1
            if self._unmapped_logged < 4:
                self._unmapped_logged += 1
                self._note(f"media from unmapped ssrc {ssrc} pt {pt}")
            return
        if self._dave_disabled:
            plaintext = payload
        elif self._dave is not None:
            plaintext = self._dave.decrypt_for(user_id, payload)
        else:
            plaintext = None
        if plaintext is None:
            self._pending += 1
            return
        self._frames_ok += 1
        self._user_frames[user_id] = self._user_frames.get(user_id, 0) + 1
        if plaintext == OPUS_SILENCE:
            self._silence += 1
        if self._on_frame is not None:
            timestamp = int.from_bytes(packet[4:8], "big")
            self._on_frame(user_id, plaintext, timestamp)

    async def _heartbeat(self, interval_ms: float) -> None:
        interval = interval_ms / 1000
        await asyncio.sleep(interval * random.random())
        while True:
            assert self._ws is not None
            await self._ws.send_str(json.dumps(
                {"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": self._seq}}
            ))
            await asyncio.sleep(interval)
