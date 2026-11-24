"""Analyze a captured video frame dump (capture/fail_XX.json).

The probe writes transport-decrypted RTP payloads, the sequence order and the
first 16 bytes of each RTP header. This script rebuilds the E2EE frame the way
the probe does and prints structural data:

  * per-packet RTP header fields (P/X/CC/marker/PT/seq/ts, extension length)
  * the reassembled NAL units and their sizes
  * the DAVE footer (nonce, unencrypted ranges, supplemental size)

No keys are involved; a tag mismatch cannot be confirmed here, but malformed
frames, wrong sequence gaps, padding, and the actual unencrypted ranges can.
Usage: python scripts/analyze_frame_dump.py capture/fail_00.json
"""
from __future__ import annotations

import base64
import json
import struct
import sys


def read_uleb(data: bytes, pos: int, end: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while pos < end:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
    raise ValueError("truncated ULEB128")


def parse_header(header: bytes) -> dict:
    if len(header) < 12:
        return {"error": "short header"}
    first, second = header[0], header[1]
    seq, ts, ssrc = struct.unpack(">HII", header[2:12])
    cc = first & 0x0F
    ext = bool(first & 0x10)
    padding = bool(first & 0x20)
    info = {
        "p": int(padding), "x": int(ext), "cc": cc,
        "marker": int(bool(second & 0x80)), "pt": second & 0x7F,
        "seq": seq, "ts": ts, "ssrc": ssrc,
    }
    if ext and len(header) >= 12 + 4 * cc + 4:
        preamble = 12 + 4 * cc
        info["ext_words"] = int.from_bytes(header[preamble + 2:preamble + 4], "big")
        info["ext_bytes"] = 4 * info["ext_words"]
    return info


def footer(frame: bytes) -> dict:
    if len(frame) < 13 or frame[-2:] != b"\xfa\xfa":
        return {"valid": False, "tail": frame[-8:].hex()}
    supplemental = frame[-3]
    start = len(frame) - supplemental
    if supplemental < 12 or start < 0:
        return {"valid": False, "supplemental": supplemental}
    tag = frame[start:start + 8]
    try:
        nonce, pos = read_uleb(frame, start + 8, len(frame) - 3)
    except ValueError:
        return {"valid": False, "supplemental": supplemental, "tag": tag.hex()}
    ranges = []
    try:
        while pos < len(frame) - 3:
            offset, pos = read_uleb(frame, pos, len(frame) - 3)
            size, pos = read_uleb(frame, pos, len(frame) - 3)
            ranges.append((offset, size))
    except ValueError:
        pass
    return {
        "valid": True, "supplemental": supplemental, "tag": tag.hex(),
        "nonce": nonce, "ranges": ranges,
        "ciphertext_bytes": len(frame) - supplemental - sum(size for _, size in ranges),
    }


def depacketize(payloads: list[bytes]) -> list[bytes]:
    nals: list[bytearray] = []
    for payload in payloads:
        nal_type = payload[0] & 0x1F
        if nal_type == 28 and len(payload) >= 2:
            if payload[1] & 0x80:
                header = (payload[0] & 0xE0) | (payload[1] & 0x1F)
                nals.append(bytearray([header]) + payload[2:])
            elif nals:
                nals[-1].extend(payload[2:])
        elif nal_type == 24:
            i = 1
            while i + 2 <= len(payload):
                size = int.from_bytes(payload[i:i + 2], "big")
                i += 2
                if size <= 0 or i + size > len(payload):
                    break
                nals.append(bytearray(payload[i:i + size]))
                i += size
        else:
            nals.append(bytearray(payload))
    return [bytes(nal) for nal in nals]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: analyze_frame_dump.py capture/fail_00.json")
    document = json.loads(open(sys.argv[1], encoding="utf-8").read())
    print("info:", document.get("info"))
    order = document["order"]
    payloads = [base64.b64decode(document["payloads"][str(seq)]) for seq in order]
    headers = {int(k): v for k, v in document.get("headers", {}).items()}
    print(f"packets: {len(payloads)}")
    for seq, payload in zip(order, payloads):
        parsed = parse_header(headers.get(seq, b""))
        parsed.pop("seq", None)
        parsed.pop("ts", None)
        parsed.pop("ssrc", None)
        print(f"  seq {seq} len {len(payload)} {parsed}")
    nals = depacketize(payloads)
    print("nals:", [(nal[0] & 0x1F, len(nal)) for nal in nals])
    frame = b"".join(b"\x00\x00\x00\x01" + nal for nal in nals)
    print("frame:", len(frame), "bytes")
    print("first16:", frame[:16].hex())
    print("footer:", footer(frame))
    # The layout alternatives the probe tries; all are printed for comparison.
    sc3 = b"".join(b"\x00\x00\x01" + nal for nal in nals)
    print("sc3 layout size:", len(sc3), "footer:", footer(sc3).get("valid"))


if __name__ == "__main__":
    main()
