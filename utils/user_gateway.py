from __future__ import annotations
import asyncio
import json
import logging
import random
import uuid
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable
import aiohttp

log = logging.getLogger(__name__)

DEFAULT_GATEWAY_URL = "wss://gateway.discord.gg/?encoding=json&v=9&compress=zlib-stream"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"

# Copied from a captured browser session. If READY or the dispatches look wrong,
# this is the first thing to experiment with.
CAPABILITIES = 1767421

# Web-client fingerprint (Chrome on Windows), from a captured browser session.
BROWSER_VERSION = "158.0.0.0"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    f"(KHTML, like Gecko) Chrome/{BROWSER_VERSION} Safari/537.36"
)
CLIENT_BUILD_NUMBER = 679109
GATEWAY_ORIGIN = "https://discord.com"
GATEWAY_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}
# permessage-deflate: aiohttp sends "permessage-deflate; client_max_window_bits"
# as Sec-WebSocket-Extensions when compress is non-zero (15 = max window).
WS_COMPRESS = 15

def build_identify(token: str, launch_id: str) -> dict:
    return {
        "token": token,
        "capabilities": CAPABILITIES,
        "properties": {
            "os": "Windows",
            "browser": "Chrome",
            "device": "",
            "device_model_name": "",
            "system_locale": "en-GB",
            "has_client_mods": False,
            "browser_user_agent": BROWSER_USER_AGENT,
            "browser_version": BROWSER_VERSION,
            "os_version": "10",
            "referrer": "",
            "referring_domain": "",
            "referrer_current": "",
            "referring_domain_current": "",
            "release_channel": "stable",
            "client_build_number": CLIENT_BUILD_NUMBER,
            "client_event_source": None,
            "client_launch_id": launch_id,
            "is_fast_connect": True,
        },
        "client_state": {"guild_versions": {}},
    }

class GatewayError(Exception):
    pass

def clean_token(token: str) -> str:
    return token.strip().strip("\"'").strip()

def token_format_hint(raw: str) -> str:
    problems = []
    if raw != raw.strip():
        problems.append("leading/trailing whitespace or a line break")
    if raw.strip()[:1] in "\"'" or raw.strip()[-1:] in "\"'":
        problems.append("quotes inside the value")
    t = clean_token(raw)
    if t.lower().startswith("bearer"):
        problems.append("a 'Bearer' prefix")
    if any(c.isspace() for c in t):
        problems.append("spaces or a line break in the middle")
    if len(t) < 50:
        problems.append(f"it's only {len(t)} characters, which is short for a user token")
    if t.count(".") != 2:
        problems.append(f"it has {t.count('.')} dots (user tokens usually have 2)")
    if problems:
        return "token format problems: " + "; ".join(problems)
    return f"token format looks normal ({len(t)} chars, 2 dots)"

@dataclass
class StreamInfo:
    stream_key: str
    rtc_server_id: str
    rtc_channel_id: str
    region: str
    endpoint: str
    user_id: str             # the watcher account (identifies on the voice socket)
    streamer_user_id: str    # whose frames we want to decrypt
    voice_session_id: str = field(repr=False)
    token: str = field(repr=False)  # short-lived stream token
    # The channel's own voice server (VOICE_SERVER_UPDATE), for receiving the
    # call audio. Empty when Discord didn't send one.
    voice_endpoint: str = ""
    voice_token: str = field(default="", repr=False)

class Watcher:
    def __init__(self, token: str, *, gateway_url: str = DEFAULT_GATEWAY_URL, on_end: Callable[[], None] | None = None) -> None:
        self._raw_token = token
        self._token = clean_token(token)
        self._gateway_url = gateway_url
        self._on_end = on_end

        self._launch_id = str(uuid.uuid4())
        self._http: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._inflater = zlib.decompressobj()
        self._buf = bytearray()
        self._seq: int | None = None
        self._tasks: list[asyncio.Task] = []
        self._closing = False
        self._watching = False

        self._session_id = ""
        self._user_id = ""
        self._voice_session_id = ""
        self._channel_id = ""
        self._stream_key = ""
        self._futures: dict[str, asyncio.Future] = {}

        self.ended = asyncio.Event()
        self.end_reason = ""  # why the watch ended (safe to show)
        # "connection_lost" (gateway dropped), "stream_ended" (STREAM_DELETE),
        # or "watcher_left" (we were removed from the voice channel).
        self.end_kind = ""

        # Names of events seen/sent (never payloads), for diagnosing drops.
        self._trace: list[str] = []
        self._close_info: tuple | None = None

    async def watch(self, *, guild_id: int, channel_id: int, target_user_id: int, timeout: float = 20.0) -> StreamInfo:
        loop = asyncio.get_running_loop()
        self._futures = {
            k: loop.create_future()
            for k in ("ready", "voice", "voice_server", "stream_create", "stream_server")
        }
        self._channel_id = str(channel_id)
        self._guild_id = str(guild_id)
        self._stream_key = f"guild:{guild_id}:{channel_id}:{target_user_id}"

        try:
            await self._connect()
            await self._wait("ready", timeout, "login")
            await self._send(4, {
                "guild_id": str(guild_id),
                "channel_id": str(channel_id),
                "self_mute": True,
                "self_deaf": False,
                "self_video": False,
                "flags": 2,
            })
            await self._wait("voice", timeout, "joining the voice channel")
            await self._send(20, {"stream_key": self._stream_key})  # STREAM_WATCH
            created = await self._wait(
                "stream_create", timeout,
                "the stream to start (is the target actually streaming?)",
            )
            server = await self._wait("stream_server", timeout, "the stream server")
            # The channel's voice server update rides alongside
            # VOICE_STATE_UPDATE. It is only needed for the optional call-audio
            # relay, so missing it must not fail the stream watch.
            try:
                voice_server = await asyncio.wait_for(
                    asyncio.shield(self._futures["voice_server"]), 3
                )
            except Exception:
                voice_server = None
                self._note("no voice server update; call audio won't be relayed")
        except BaseException:
            await self.close()
            raise

        self._watching = True
        return StreamInfo(
            stream_key=self._stream_key,
            rtc_server_id=created["rtc_server_id"],
            rtc_channel_id=created["rtc_channel_id"],
            region=created.get("region", "?"),
            endpoint=server["endpoint"],
            user_id=self._user_id,
            streamer_user_id=str(target_user_id),
            voice_session_id=self._voice_session_id,
            token=server["token"],
            voice_endpoint=str((voice_server or {}).get("endpoint") or ""),
            voice_token=str((voice_server or {}).get("token") or ""),
        )

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        await self._leave_voice()
        current = asyncio.current_task()
        for t in self._tasks:
            if t is not current:
                t.cancel()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._http is not None:
            await self._http.close()

    async def _leave_voice(self) -> None:
        ws = self._ws
        if ws is None or ws.closed or not self._voice_session_id:
            return
        try:
            await asyncio.wait_for(
                self._send(4, {
                    "guild_id": self._guild_id,
                    "channel_id": None,
                    "self_mute": True,
                    "self_deaf": False,
                }),
                2.0,
            )
            self._note("sent op4 (leave voice)")
        except Exception:
            log.info("watcher: explicit voice leave failed; closing the gateway anyway")

    async def _check_token(self) -> None:
        assert self._http is not None
        try:
            async with self._http.get(
                "https://discord.com/api/v9/users/@me",
                headers={"Authorization": self._token},
            ) as r:
                status = r.status
        except aiohttp.ClientError as e:
            raise GatewayError(f"Couldn't reach the Discord API: {e}") from e
        if status == 401:
            raise GatewayError(
                "Token is invalid (API returned 401). The session it came from was "
                "logged out or reset; grab a fresh token. "
                + token_format_hint(self._raw_token)
            )
        if status == 403:
            raise GatewayError("Token accepted but the account is locked or needs verification (403).")
        if status != 200:
            raise GatewayError(f"Unexpected API status {status} checking the token.")
        self._note("token check ok")

    async def _connect(self) -> None:
        self._http = aiohttp.ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
        await self._check_token()
        try:
            self._ws = await self._http.ws_connect(
                self._gateway_url,
                max_msg_size=0,
                origin=GATEWAY_ORIGIN,
                headers=GATEWAY_HEADERS,
                compress=WS_COMPRESS,
            )
        except aiohttp.ClientError as e:
            raise GatewayError(f"Couldn't reach the gateway: {e}") from e
        self._tasks.append(asyncio.create_task(self._read_loop()))

    def _note(self, event: str) -> None:
        self._trace.append(event)
        log.info("gateway: %s", event)

    async def _send(self, op: int, d: Any) -> None:
        assert self._ws is not None
        self._note(f"sent op{op}")
        await self._ws.send_str(json.dumps({"op": op, "d": d}))

    async def _wait(self, name: str, timeout: float, what: str) -> Any:
        try:
            return await asyncio.wait_for(asyncio.shield(self._futures[name]), timeout)
        except asyncio.TimeoutError:
            raise GatewayError(f"Timed out waiting for {what}.") from None

    def _resolve(self, name: str, value: Any) -> None:
        fut = self._futures.get(name)
        if fut is not None and not fut.done():
            fut.set_result(value)

    def _fail_pending(self, exc: Exception) -> None:
        for fut in self._futures.values():
            if not fut.done():
                fut.set_exception(exc)
            fut.add_done_callback(lambda f: f.cancelled() or f.exception())

    async def _read_loop(self) -> None:
        assert self._ws is not None
        error: Exception | None = None
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    self._buf.extend(msg.data)
                    if msg.data[-4:] != ZLIB_SUFFIX:
                        continue
                    text = self._inflater.decompress(bytes(self._buf))
                    self._buf.clear()
                    payload = json.loads(text)
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                  aiohttp.WSMsgType.CLOSED):
                    self._close_info = (msg.data, msg.extra)
                    continue
                else:
                    continue
                await self._handle(payload)
        except Exception as e:  # malformed frame, unexpected payload shape, ...
            error = e
        finally:
            code = self._ws.close_code
            ws_exc = self._ws.exception()
            if error is not None:
                reason = f"Gateway error: {type(error).__name__}"
                extra = []
            elif code == 4004:
                reason = "Authentication failed (token rejected)."
                extra = [token_format_hint(self._raw_token)]
            else:
                reason = f"Gateway connection closed (code {code})."
                extra = []
            if self._close_info and self._close_info[1]:
                extra.append(f"close reason: {self._close_info[1]}")
            if ws_exc is not None:
                extra.append(f"socket error: {type(ws_exc).__name__}")
            extra.append("events before drop: " + (", ".join(self._trace[-8:]) or "none"))
            reason += " (" + "; ".join(extra) + ")"
            self._fail_pending(GatewayError(reason))
            if self._watching and not self._closing:
                self.end_reason = reason
                if not self.end_kind:
                    self.end_kind = "connection_lost"
                log.info("watcher ended: %s", reason)
                self._signal_end()

    async def _handle(self, payload: dict) -> None:
        op = payload["op"]
        self._note(f"got {payload['t']}" if op == 0 else f"got op{op}")
        if payload.get("s") is not None:
            self._seq = payload["s"]

        if op == 10:  # HELLO
            interval = payload["d"]["heartbeat_interval"]
            self._tasks.append(asyncio.create_task(self._heartbeat(interval)))
            await self._send(2, build_identify(self._token, self._launch_id))
        elif op == 1:  # server asks for an immediate heartbeat
            await self._send(1, self._seq)
        elif op == 9:
            self._fail_pending(GatewayError("Invalid session (token rejected?)."))
        elif op == 7:
            self._fail_pending(GatewayError("Gateway asked us to reconnect."))
        elif op == 0:
            self._dispatch(payload["t"], payload["d"])

    def _dispatch(self, t: str, d: dict) -> None:
        if t == "READY":
            self._session_id = d["session_id"]
            self._user_id = d["user"]["id"]
            self._resolve("ready", True)
        elif t == "VOICE_STATE_UPDATE":
            if d.get("user_id") != self._user_id:
                return
            if d.get("channel_id") == self._channel_id:
                self._voice_session_id = d.get("session_id") or self._session_id
                self._resolve("voice", True)
            elif self._watching and not self._closing:
                where = "moved to another channel" if d.get("channel_id") else "disconnected"
                self.end_kind = "watcher_left"
                self.end_reason = (
                    f"watcher was {where} from the voice channel; events before: "
                    + ", ".join(self._trace[-8:])
                )
                log.info("watcher ended: %s", self.end_reason)
                self._signal_end()
        elif t == "VOICE_SERVER_UPDATE":
            if str(d.get("guild_id")) == self._guild_id:
                self._resolve("voice_server", d)
        elif t == "STREAM_CREATE" and d.get("stream_key") == self._stream_key:
            self._resolve("stream_create", d)
        elif t == "STREAM_SERVER_UPDATE" and d.get("stream_key") == self._stream_key:
            self._resolve("stream_server", d)
        elif t == "STREAM_DELETE" and d.get("stream_key") == self._stream_key:
            self.end_kind = "stream_ended"
            self.end_reason = (
                f"STREAM_DELETE (reason={d.get('reason')!r}, unavailable={d.get('unavailable')!r}); "
                "events before: " + ", ".join(self._trace[-8:])
            )
            log.info("watcher ended: %s", self.end_reason)
            self._signal_end()

    def _signal_end(self) -> None:
        if self.ended.is_set():
            return
        self.ended.set()
        if self._on_end is not None:
            self._on_end()

    async def _heartbeat(self, interval_ms: int) -> None:
        interval = interval_ms / 1000
        await asyncio.sleep(interval * random.random())
        while True:
            await self._send(1, self._seq)
            await asyncio.sleep(interval)
