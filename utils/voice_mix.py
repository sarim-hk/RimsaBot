from __future__ import annotations
import asyncio
import logging
import random
import time
from typing import Any, Callable

try:
    import audioop
except ImportError:  # Python 3.13+ without the audioop-lts backport
    audioop = None

log = logging.getLogger(__name__)

# Discord voice is Opus, 48 kHz stereo, in 20 ms frames.
SAMPLE_RATE = 48000
CHANNELS = 2
TICK_SAMPLES = 960          # 20 ms
TICK_SECONDS = TICK_SAMPLES / SAMPLE_RATE
TICK_BYTES = TICK_SAMPLES * CHANNELS * 2

try:
    import av
except ImportError:  # the relay still works, just without call audio
    av = None

def available() -> bool:
    return av is not None and audioop is not None

class _Speaker:
    """One sender's Opus decoder and playout buffer.

    Frames are placed on an absolute sample timeline (derived from their RTP
    timestamps) so several speakers line up with each other on the 20 ms mix
    grid, and so a lost packet becomes a hole rather than a 20 ms jump.
    """

    def __init__(self) -> None:
        self.decoder = av.CodecContext.create("libopus", "r")
        self.resampler = av.AudioResampler(
            format="s16", layout="stereo", rate=SAMPLE_RATE
        )
        self.base = 0            # absolute sample index of buf[0]
        self.buf = bytearray()
        self.anchor_ts: int | None = None
        self.anchor_sample = 0
        self.last_update = 0.0
        self.errors = 0

    def decode(self, opus: bytes) -> bytes:
        try:
            frames = self.decoder.decode(av.Packet(opus))
            out = bytearray()
            for frame in frames:
                for resampled in self.resampler.resample(frame):
                    out += bytes(resampled.planes[0])[: resampled.samples * CHANNELS * 2]
        except Exception as e:
            self.errors += 1
            log.warning("voice mixer: Opus decode failed: %s", type(e).__name__)
            return b""
        return bytes(out)

    def add(self, start: int, pcm: bytes) -> None:
        if not pcm:
            return
        if not self.buf:
            self.base = start
            self.buf.extend(pcm)
            return
        if start < self.base:
            skip = self.base - start
            if skip * 4 >= len(pcm):
                return  # entirely in the past
            pcm = pcm[skip * 4:]
            start = self.base
        buf_end = self.base + len(self.buf) // 4
        if start > buf_end:
            self.buf.extend(b"\x00" * ((start - buf_end) * 4))
        offset = (start - self.base) * 4
        end = offset + len(pcm)
        if end > len(self.buf):
            self.buf.extend(b"\x00" * (end - len(self.buf)))
        self.buf[offset:end] = pcm

    def take(self, pos: int, samples: int) -> bytes | None:
        """Return this speaker's audio for [pos, pos+samples), or None."""
        end = pos + samples
        if not self.buf:
            return None
        buf_end = self.base + len(self.buf) // 4
        if buf_end <= pos:
            self.buf.clear()
            return None
        if self.base >= end:
            return None  # nothing for this tick yet
        a = max(pos, self.base)
        b = min(end, buf_end)
        out: bytearray | None = None
        if b > a:
            out = bytearray(samples * CHANNELS * 2)
            src = (a - self.base) * 4
            dst = (a - pos) * 4
            out[dst : dst + (b - a) * 4] = self.buf[src : src + (b - a) * 4]
        cut = end - self.base
        if cut >= len(self.buf) // 4:
            self.buf.clear()
        else:
            del self.buf[: cut * 4]
        self.base = end
        return bytes(out) if out is not None else None

class VoiceMixer:
    """Decodes every speaker, mixes them on a 20 ms grid and re-encodes the
    result for the sender account's microphone."""

    PLAYOUT_TICKS = 4            # ~80 ms buffering for relay + network jitter
    MAX_AHEAD_SAMPLES = 3 * SAMPLE_RATE
    MAX_LATE_SAMPLES = SAMPLE_RATE // 2
    MAX_SPEAKERS = 64
    SPEAKER_IDLE_SECONDS = 60.0
    CLEANUP_SECONDS = 5.0

    def __init__(self, sink: Callable[[bytes, int], None], *, bitrate: int = 64000) -> None:
        if av is None or audioop is None:
            raise RuntimeError("PyAV (with libopus) or audioop is not installed")
        self._sink = sink
        self._speakers: dict[str, _Speaker] = {}
        self._sample_pos = 0     # start of the next tick's 20 ms window
        self._rtp_base = random.getrandbits(32)
        self._closed = False
        self._last_cleanup = 0.0
        self._encoder = av.CodecContext.create("libopus", "w")
        self._encoder.sample_rate = SAMPLE_RATE
        self._encoder.layout = "stereo"
        self._encoder.format = "s16"
        self._encoder.bit_rate = bitrate
        self.ticks = 0
        self.mixed_frames = 0
        self.frames_out = 0
        self.errors = 0

    def feed(self, user_id: str, opus: bytes, rtp_ts: int) -> None:
        if self._closed or not opus:
            return
        speaker = self._speakers.get(user_id)
        if speaker is None:
            if len(self._speakers) >= self.MAX_SPEAKERS:
                self._evict_oldest()
            try:
                speaker = _Speaker()
            except Exception as e:
                self.errors += 1
                log.warning("voice mixer: no Opus decoder: %s", type(e).__name__)
                return
            self._speakers[user_id] = speaker
        pcm = speaker.decode(opus)
        if not pcm:
            return
        start = self._place(speaker, rtp_ts)
        speaker.add(start, pcm)
        speaker.last_update = time.monotonic()

    def _place(self, speaker: _Speaker, rtp_ts: int) -> int:
        ahead = self._sample_pos + self.PLAYOUT_TICKS * TICK_SAMPLES
        if speaker.anchor_ts is None:
            speaker.anchor_ts = rtp_ts
            speaker.anchor_sample = ahead
            return ahead
        delta = (rtp_ts - speaker.anchor_ts) & 0xFFFFFFFF
        if delta >= 0x80000000 or delta > self.MAX_AHEAD_SAMPLES:
            # A restarted stream, a long silence or a dead clock: rebase.
            speaker.buf.clear()
            speaker.anchor_ts = rtp_ts
            speaker.anchor_sample = ahead
            return ahead
        start = speaker.anchor_sample + delta
        if start < self._sample_pos - self.MAX_LATE_SAMPLES:
            # Too late to play without delaying everyone else: rebase.
            speaker.buf.clear()
            speaker.anchor_ts = rtp_ts
            speaker.anchor_sample = ahead
            return ahead
        return start

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        next_at = loop.time() + TICK_SECONDS
        while not self._closed:
            delay = next_at - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            if self._closed:
                return
            self._tick()
            next_at += TICK_SECONDS
            now = loop.time()
            if next_at < now:
                # The loop stalled: catch up one tick at a time rather than
                # bursting several frames at once.
                next_at = now

    def _tick(self) -> None:
        pos = self._sample_pos
        self._sample_pos += TICK_SAMPLES
        self.ticks += 1
        mixed: bytes | None = None
        for speaker in list(self._speakers.values()):
            chunk = speaker.take(pos, TICK_SAMPLES)
            if chunk is None:
                continue
            mixed = chunk if mixed is None else audioop.add(mixed, chunk, 2)
        if mixed is not None and len(mixed) == TICK_BYTES:
            self._emit(mixed, pos)
        self._cleanup()

    def _emit(self, pcm: bytes, pos: int) -> None:
        frame = av.AudioFrame(format="s16", layout="stereo", samples=TICK_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.planes[0].update(pcm)
        try:
            packets = self._encoder.encode(frame)
        except Exception as e:
            self.errors += 1
            log.warning("voice mixer: Opus encode failed: %s", type(e).__name__)
            return
        if not packets:
            return
        self.mixed_frames += 1
        ts = (self._rtp_base + pos) & 0xFFFFFFFF
        for packet in packets:
            try:
                self._sink(bytes(packet), ts)
            except Exception:
                self.errors += 1
            else:
                self.frames_out += 1

    def _cleanup(self) -> None:
        now = time.monotonic()
        if now - self._last_cleanup < self.CLEANUP_SECONDS:
            return
        self._last_cleanup = now
        for user_id, speaker in list(self._speakers.items()):
            if now - speaker.last_update > self.SPEAKER_IDLE_SECONDS:
                del self._speakers[user_id]

    def _evict_oldest(self) -> None:
        oldest = min(self._speakers, key=lambda k: self._speakers[k].last_update)
        self._speakers.pop(oldest, None)

    def close(self) -> None:
        self._closed = True

    def stats(self) -> dict[str, Any]:
        return {
            "running": not self._closed,
            "speakers": len(self._speakers),
            "ticks": self.ticks,
            "mixed_frames": self.mixed_frames,
            "frames_out": self.frames_out,
            "errors": self.errors,
        }

    def summary(self) -> str:
        return (
            f"mixer: {len(self._speakers)} speaker(s), "
            f"{self.frames_out} frames sent, {self.errors} error(s)"
        )
