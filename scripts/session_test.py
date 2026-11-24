"""Phased session probe: find what makes Discord revoke the watcher session.

Connects the user gateway with the same Identify the bot uses and then runs
phases back to back:

  1. idle    -- authenticated, doing nothing
  2. voice   -- sends op 4 to join the given voice channel
  3. stream  -- sends op 20 STREAM_WATCH for the given stream key

Each phase is timed. The script stops at the first gateway close and reports
the close code, the phase it happened in, and whether GET /users/@me still
accepts the token afterwards. The token is read from config.json and never
printed; payload contents are not logged either.

Usage:
  python scripts/session_test.py --guild 520319280747642950 \
      --channel 932923540775239700 --streamer 297111993352060948
  python scripts/session_test.py ... --durations 45,30,30

Requires a fresh WATCHER_TOKEN in config.json. Each phase may burn it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
import zlib
from pathlib import Path

import aiohttp

GATEWAY_URL = "wss://gateway.discord.gg/?encoding=json&v=9&compress=zlib-stream"
ZLIB_SUFFIX = b"\x00\x00\xff\xff"
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.user_gateway import (  # noqa: E402
    BROWSER_USER_AGENT, GATEWAY_HEADERS, GATEWAY_ORIGIN, WS_COMPRESS, build_identify,
)


def load_token() -> str:
    with open(ROOT / "config.json", encoding="utf-8") as f:
        return json.load(f)["WATCHER_TOKEN"].strip().strip("\"'").strip()


async def token_status(http: aiohttp.ClientSession, token: str) -> int | str:
    try:
        async with http.get(
            "https://discord.com/api/v9/users/@me",
            headers={"Authorization": token},
        ) as r:
            return r.status
    except aiohttp.ClientError as e:
        return type(e).__name__


class Probe:
    def __init__(self, token: str, guild: int, channel: int, streamer: int) -> None:
        self._token = token
        self._launch_id = str(uuid.uuid4())
        self._guild = guild
        self._channel = channel
        self._streamer = streamer
        self._http: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._inflater = zlib.decompressobj()
        self._buf = bytearray()
        self._seq: int | None = None
        self._heartbeat: asyncio.Task | None = None
        self._heartbeat_interval = 41.25
        self._tasks: list[asyncio.Task] = []
        self._ready = asyncio.Event()
        self._closed = asyncio.Event()
        self._closing = False
        self.close_code: int | None = None
        self.events: list[str] = []
        self.started = time.monotonic()

    # ---------------------------------------------------------------- output

    def log(self, text: str) -> None:
        elapsed = time.monotonic() - self.started
        print(f"  +{elapsed:6.1f}s  {text}", flush=True)

    # ---------------------------------------------------------------- client

    async def connect(self) -> bool:
        self._http = aiohttp.ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
        status = await token_status(self._http, self._token)
        self.log(f"token status before: {status}")
        if status != 200:
            self.log("token is not accepted; put a fresh WATCHER_TOKEN in config.json")
            return False
        self._ws = await self._http.ws_connect(
            GATEWAY_URL, max_msg_size=0, origin=GATEWAY_ORIGIN,
            headers=GATEWAY_HEADERS, compress=WS_COMPRESS,
        )
        self._tasks.append(asyncio.create_task(self._read_loop()))
        try:
            await asyncio.wait_for(self._ready.wait(), 15)
            return True
        except asyncio.TimeoutError:
            self.log("timed out waiting for READY")
            return False

    async def _send(self, op: int, d: object) -> None:
        assert self._ws is not None
        await self._ws.send_str(json.dumps({"op": op, "d": d}))

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    self._buf.extend(msg.data)
                    if msg.data[-4:] != ZLIB_SUFFIX:
                        continue
                    payload = json.loads(self._inflater.decompress(bytes(self._buf)))
                    self._buf.clear()
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(msg.data)
                else:
                    continue
                await self._handle(payload)
        except Exception as e:
            self.log(f"read loop error: {type(e).__name__}")
        finally:
            self.close_code = self._ws.close_code
            self._closed.set()
            self.log(f"GATEWAY CLOSED code {self.close_code}")

    async def _handle(self, payload: dict) -> None:
        op = payload.get("op")
        if payload.get("s") is not None:
            self._seq = payload["s"]
        if op == 10:
            self._heartbeat_interval = payload["d"]["heartbeat_interval"] / 1000
            self._heartbeat = asyncio.create_task(self._beat())
            self._tasks.append(self._heartbeat)
            await self._send(2, build_identify(self._token, self._launch_id))
            self.log("identify sent")
        elif op == 11:
            self.events.append("heartbeat ack")
        elif op == 0:
            name = payload["t"]
            self.events.append(name)
            detail = ""
            d = payload.get("d") or {}
            if name in ("VOICE_STATE_UPDATE", "VOICE_SERVER_UPDATE"):
                detail = f" channel={d.get('channel_id')} user={d.get('user_id')}"
            elif name in ("STREAM_CREATE", "STREAM_SERVER_UPDATE", "STREAM_UPDATE"):
                detail = f" key={d.get('stream_key')} region={d.get('region')}"
            elif name == "SESSIONS_REPLACE":
                sessions = d if isinstance(d, list) else []
                summary = [
                    {k: (s.get("client_info") or {}).get(k)
                     for k in ("os", "client", "device")}
                    for s in sessions
                ]
                detail = f" {len(sessions)} session(s): {summary}"
            self.log(f"{name}{detail}")
            if name == "READY":
                user = (d.get("user") or {}).get("id")
                self.log(f"READY as user {user}")
                self._ready.set()
        elif op == 9:
            self.log("op9 invalid session")
        elif op == 7:
            self.log("op7 reconnect request")

    async def _beat(self) -> None:
        await asyncio.sleep(self._heartbeat_interval * 0.5)
        while True:
            await self._send(1, self._seq)
            await asyncio.sleep(self._heartbeat_interval)

    # ---------------------------------------------------------------- phases

    async def phase(self, name: str, duration: float, action=None) -> bool:
        if action is not None:
            await action()
        self.log(f"phase {name}: start ({duration:.0f}s)")
        try:
            await asyncio.wait_for(self._closed.wait(), duration)
        except asyncio.TimeoutError:
            self.log(f"phase {name}: survived")
            if self._http is not None:
                self.log(f"token status after {name}: {await token_status(self._http, self._token)}")
            return True
        self.log(f"phase {name}: closed after close code {self.close_code}")
        if self._http is not None:
            self.log(f"token status after {name}: {await token_status(self._http, self._token)}")
        return False

    async def join_voice(self) -> None:
        await self._send(4, {
            "guild_id": str(self._guild),
            "channel_id": str(self._channel),
            "self_mute": True,
            "self_deaf": False,
            "self_video": False,
            "flags": 2,
        })

    async def watch_stream(self) -> None:
        key = f"guild:{self._guild}:{self._channel}:{self._streamer}"
        await self._send(20, {"stream_key": key})

    async def close(self) -> None:
        self._closing = True
        current = asyncio.current_task()
        for t in self._tasks:
            if t is not current:
                t.cancel()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._http is not None:
            await self._http.close()


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--guild", type=int)
    ap.add_argument("--channel", type=int)
    ap.add_argument("--streamer", type=int)
    ap.add_argument("--phases", default="idle,voice,stream",
                    help="subset of idle,voice,stream (default all)")
    ap.add_argument("--durations", default="45,30,30",
                    help="seconds per phase; one value or one per phase")
    args = ap.parse_args()

    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    if not phases or any(p not in ("idle", "voice", "stream") for p in phases):
        ap.error("--phases must be a subset of idle,voice,stream")
    if ("voice" in phases or "stream" in phases) and (
        args.guild is None or args.channel is None
    ):
        ap.error("--guild and --channel are required for voice/stream phases")
    if "stream" in phases and args.streamer is None:
        ap.error("--streamer is required for the stream phase")

    durations = [float(x) for x in args.durations.split(",")]
    if len(durations) == 1:
        durations *= len(phases)
    if len(durations) != len(phases):
        ap.error("--durations needs one value or one per phase")

    probe = Probe(load_token(), args.guild or 0, args.channel or 0, args.streamer or 0)
    actions = {
        "idle": None,
        "voice": probe.join_voice,
        "stream": probe.watch_stream,
    }
    try:
        if not await probe.connect():
            return 1
        for name, duration in zip(phases, durations):
            if not await probe.phase(name, duration, actions[name]):
                return 0
        print("all requested phases survived; no revocation observed")
        return 0
    finally:
        await probe.close()
        print("recent events:", ", ".join(probe.events[-12:]) or "none")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
