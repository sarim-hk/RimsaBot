from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    # The type checker only sees the module, so every dave.X access below
    # checks against dave.py's real API instead of "None".
    import dave
else:
    try:
        import dave
    except ImportError:  # probe still runs (milestone 3a behaviour)
        dave = None

OPUS_PAYLOAD_TYPE = 120

AES_MODE = "aead_aes256_gcm_rtpsize"
# Also offered by the voice server, but `cryptography` has no XChaCha20-Poly1305;
# we always prefer and select AES-GCM, so only that mode is implemented.
XCHACHA_MODE = "aead_xchacha20_poly1305_rtpsize"

def available() -> bool:
    return dave is not None

def rtp_header_size(packet: bytes) -> int:
    size = 12 + 4 * (packet[0] & 0x0F)
    if packet[0] & 0x10:
        size += 4
    return size

def _read_uleb(data: bytes, pos: int, limit: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while pos < limit:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 35:
            break
    raise ValueError("truncated ULEB128")

def frame_metadata(payload: bytes) -> tuple[int | None, list[tuple[int, int]]]:
    if len(payload) < 13 or payload[-2:] != b"\xfa\xfa":
        return None, []
    supplemental = payload[-3]
    start = len(payload) - supplemental
    if supplemental < 12 or start < 0:
        return None, []
    limit = len(payload) - 3  # excludes the size byte and marker
    pos = start + 8  # skip the tag
    try:
        nonce, pos = _read_uleb(payload, pos, limit)
    except ValueError:
        return None, []
    ranges: list[tuple[int, int]] = []
    try:
        while pos < limit:
            offset, pos = _read_uleb(payload, pos, limit)
            size, pos = _read_uleb(payload, pos, limit)
            ranges.append((offset, size))
    except ValueError:
        pass
    return nonce, ranges

def frame_nonce(payload: bytes) -> int | None:
    if len(payload) < 13 or payload[-2:] != b"\xfa\xfa":
        return None
    supplemental = payload[-3]
    start = len(payload) - supplemental
    # minimum: tag(8) + one nonce byte + size byte + marker(2)
    if supplemental < 12 or start < 0 or start + 8 > len(payload) - 3:
        return None
    value = 0
    shift = 0
    for i in range(start + 8, len(payload) - 3):
        byte = payload[i]
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value
        shift += 7
        if shift > 28:
            break
    return None

class TransportCrypto:
    def __init__(self, mode: str, secret_key: bytes) -> None:
        self.mode = mode
        if mode != AES_MODE:
            raise ValueError(f"unsupported transport mode {mode!r} (only {AES_MODE})")
        self._aead = AESGCM(secret_key)
        self._nonce_tail = b"\x00" * 8

    def decrypt_packet(self, packet: bytes) -> tuple[bytes, bytes, int] | None:
        if len(packet) < 16 or packet[0] >> 6 != 2:
            return None
        size = rtp_header_size(packet)
        if len(packet) < size + 4 + 16:
            return None
        header = packet[:size]
        counter = packet[-4:]
        nonce = counter + self._nonce_tail
        try:
            plaintext = self._aead.decrypt(nonce, packet[size:-4], header)
        except InvalidTag:
            return None
        extension_bytes = 0
        if packet[0] & 0x10:  # extension preamble is the last 4 header bytes
            extension_bytes = 4 * int.from_bytes(header[-2:], "big")
            if extension_bytes > len(plaintext):
                return None
            plaintext = plaintext[extension_bytes:]
        if packet[0] & 0x20:  # RTP padding: inside the encryption, at the end
            pad = plaintext[-1] if plaintext else 0
            if 1 <= pad <= len(plaintext):
                plaintext = plaintext[:-pad]
        return header, plaintext, extension_bytes

    def encrypt_packet(self, header: bytes, payload: bytes, counter: int) -> bytes:
        counter_bytes = (counter & 0xFFFFFFFF).to_bytes(4, "big")
        nonce = counter_bytes + self._nonce_tail
        return header + self._aead.encrypt(nonce, payload, header) + counter_bytes

@dataclass
class DaveAction:
    op: int
    data: dict[str, Any] | None = None
    binary: bytes | None = None

class DaveState:
    INIT_TRANSITION_ID = 0
    NEW_GROUP_EPOCH = 1
    DISABLED_VERSION = 0

    def __init__(self, *, group_id: int, self_user_id: str, streamer_user_id: str | None = None, self_encryptor: Any | None = None, per_user: bool = False) -> None:
        if dave is None:
            raise RuntimeError("dave.py is not installed")
        self._session = dave.Session(self._on_mls_failure)
        # Streams have one sender (the streamer) and use a single decryptor.
        # A voice channel has one ratchet per speaking user, so `per_user`
        # swaps the single decryptor for a dict keyed by user id.
        self._per_user = per_user
        self._decryptor = None if per_user else dave.Decryptor()
        self._decryptors: dict[str, Any] = {}
        # Sender side: our own per-epoch ratchet goes into this encryptor.
        self._encryptor = self_encryptor
        self._group_id = int(group_id)
        self._self_id = str(self_user_id)
        self._streamer_id = str(streamer_user_id) if streamer_user_id is not None else ""
        # Allowlist of users who may join the MLS group. op11/op13 (clients
        # connect/disconnect) keep it in sync; rosters from commits/welcomes
        # add to it. A welcome containing an unknown user is rejected.
        self._recognized: set[str] = {self._self_id}
        if self._streamer_id:
            self._recognized.add(self._streamer_id)
        self._transient_keys: dict[int, Any] = {}
        self._prepared: dict[int, int] = {}
        # A Go Live group is occupied by the streamer. Existing members commit
        # Add(us) and welcome us; a local-group commit from us would race them
        # and be rejected by the gateway / the streamer's client. A voice
        # channel is the same: existing members add and welcome us.
        self._waiting_for_welcome = per_user or self._streamer_id != self._self_id

        self.protocol_version = 0
        self.initialized = False
        self.has_ratchet = False
        self.own_ratchet: Any | None = None
        self.key_packages_sent = 0
        self.welcomes = 0
        self.commits = 0
        self.transitions = 0
        self.trace: list[str] = []

    def _note(self, event: str) -> None:
        self.trace.append(event)
        log.info("dave: %s", event)

    def _on_mls_failure(self, source: str, reason: str) -> None:
        log.warning("MLS failure: %s - %s", source, reason)
        self.trace.append("mls failure")

    def add_recognized_users(self, user_ids: list[Any]) -> None:
        if any(str(user_id) != self._self_id for user_id in user_ids):
            self._waiting_for_welcome = True
        self._recognized.update(str(user_id) for user_id in user_ids)
        if user_ids:
            self._note(
                f"clients connect: {len(user_ids)} ids, "
                f"{len(self._recognized)} recognized"
            )

    def remove_recognized_user(self, user_id: Any) -> None:
        user_id = str(user_id)
        if user_id != self._self_id:
            self._recognized.discard(user_id)
            self._decryptors.pop(user_id, None)

    def _add_roster(self, roster: dict[int, list[int]]) -> None:
        added: list[str] = []
        for user_id, entry in roster.items():
            if entry:
                self._recognized.add(str(user_id))
                added.append(str(user_id))
            else:
                self._recognized.discard(str(user_id))
                self._decryptors.pop(str(user_id), None)
        if added:
            self._note(
                f"roster additions: {len(added)} users, "
                f"streamer listed: {self._streamer_id in added}"
            )

    def on_session_description(self, d: dict[str, Any]) -> list[DaveAction]:
        version = int(d.get("dave_protocol_version") or 0)
        self.protocol_version = version
        if version <= self.DISABLED_VERSION:
            self._note("protocol version 0: media is not end-to-end encrypted")
            return []
        return self._init(version)

    def _init(self, version: int) -> list[DaveAction]:
        key = self._transient_keys.get(version)
        if key is None:
            key = self._transient_keys[version] = dave.SignatureKeyPair.generate(version)
        try:
            self._session.init(version, self._group_id, self._self_id, key)
            key_package = self._session.get_marshalled_key_package()
        except Exception as e:
            self._note(f"init failed: {type(e).__name__}")
            return []
        self.initialized = True
        self.has_ratchet = False
        if self._encryptor is not None:
            try:
                self._encryptor.set_key_ratchet(None)
            except Exception:
                pass
        if key_package is None:
            self._note("init produced no key package")
            return []
        self.key_packages_sent += 1
        self._note(
            f"initialized v{version} group {self._group_id}, "
            f"key package sent ({len(key_package)} B)"
        )
        return [DaveAction(26, binary=key_package)]

    def on_prepare_epoch(self, epoch: int, version: int) -> list[DaveAction]:
        self.protocol_version = version
        if epoch != self.NEW_GROUP_EPOCH:
            # Existing group changing protocol version; executed via op21/op22.
            try:
                self._session.set_protocol_version(version)
            except Exception as e:
                self._note(f"protocol version change rejected: {type(e).__name__}")
            self._note(f"prepare epoch {epoch} (version {version})")
            return []
        if self._session.has_established_group():
            # Go Live groups exist only while viewers watch: after the last one
            # leaves, a new viewer re-creates the group and epoch 1 arrives
            # again. The old epoch cannot parse the new group's proposals
            # ("PublicMessage not for this epoch"), so start over and offer a
            # fresh key package instead of staying on the dead group.
            self._note("prepare epoch 1 for a re-created group: resetting")
            self._reset_group()
        return self._init(version)

    def _reset_group(self) -> None:
        try:
            self._session.reset()
        except Exception as e:
            self._note(f"session reset failed: {type(e).__name__}")
        self.has_ratchet = False
        self.own_ratchet = None
        self._prepared.clear()
        self._decryptors.clear()
        # Epoch 1 means we are a pending (re)joiner again; let an existing
        # member commit the Add and welcome us.
        self._waiting_for_welcome = True
        if self._encryptor is not None:
            try:
                self._encryptor.set_key_ratchet(None)
            except Exception:
                pass

    def on_external_sender(self, payload: bytes) -> None:
        try:
            self._session.set_external_sender(payload)
        except Exception as e:
            self._note(f"external sender rejected: {type(e).__name__}")
            return
        self._note("external sender set")

    def on_proposals(self, payload: bytes) -> list[DaveAction]:
        try:
            commit_welcome = self._session.process_proposals(payload, self._recognized)
        except Exception as e:
            self._note(f"proposals rejected: {type(e).__name__}")
            return []
        if commit_welcome is None:
            self._note("proposals processed (not the committer)")
            return []
        if self._waiting_for_welcome and not self._session.has_established_group():
            # Go Live: we are the pending joiner; let an existing member
            # (the streamer) commit the Add and welcome us.
            self._note("proposals queued; waiting for a welcome, not committing")
            return []
        self._note(f"commit + welcome sent ({len(commit_welcome)} B)")
        return [DaveAction(28, binary=commit_welcome)]

    def on_commit(self, transition_id: int, commit: bytes) -> list[DaveAction]:
        try:
            result = self._session.process_commit(commit)
        except Exception as e:
            self._note(f"commit {transition_id} rejected: {type(e).__name__}")
            return self._recover(transition_id)
        if result == dave.RejectType.ignored:
            self._note(f"commit {transition_id} ignored")
            return []
        if result == dave.RejectType.failed:
            self._note(f"commit {transition_id} failed")
            return self._recover(transition_id)
        self.commits += 1
        self._add_roster(result)
        self._note(f"commit {transition_id} applied")
        return self.prepare_transition(transition_id, self._session.get_protocol_version())

    def on_welcome(self, transition_id: int, welcome: bytes) -> list[DaveAction]:
        try:
            roster = self._session.process_welcome(welcome, self._recognized)
        except Exception as e:
            self._note(f"welcome {transition_id} rejected: {type(e).__name__}")
            return self._recover(transition_id)
        if roster is None:
            self._note(f"welcome {transition_id} failed")
            return self._recover(transition_id)
        self.welcomes += 1
        self._add_roster(roster)
        self._note(f"joined group via welcome {transition_id}")
        return self.prepare_transition(transition_id, self._session.get_protocol_version())

    def _recover(self, transition_id: int) -> list[DaveAction]:
        actions = [DaveAction(31, data={"transition_id": transition_id})]
        if self.protocol_version > self.DISABLED_VERSION:
            actions += self._init(self.protocol_version)
        return actions

    def prepare_transition(self, transition_id: int, version: int) -> list[DaveAction]:
        self._prepared[transition_id] = version
        if transition_id == self.INIT_TRANSITION_ID:
            self.execute_transition(transition_id)
            return []
        self._note(f"ready for transition {transition_id}")
        return [DaveAction(23, data={"transition_id": transition_id})]

    def execute_transition(self, transition_id: int) -> None:
        version = self._prepared.pop(transition_id, None)
        if version is None:
            self._note(f"execute transition {transition_id}: not prepared")
            return
        if version == self.DISABLED_VERSION:
            self._session.reset()
            self.has_ratchet = False
            self._note("DAVE disabled, media is plain transport")
            return
        if not self._session.has_established_group():
            self._note(f"execute transition {transition_id}: group not established")
            return
        if self._per_user:
            self._install_voice_ratchets(transition_id)
            return
        try:
            ratchet = self._session.get_key_ratchet(self._streamer_id)
        except Exception as e:
            self._note(f"ratchet unavailable: {type(e).__name__}")
            return
        if ratchet is None:
            self._note("streamer has no ratchet in this group")
            return
        self._decryptor.transition_to_key_ratchet(ratchet)
        self.has_ratchet = True
        self.transitions += 1
        self._note(f"streamer ratchet installed (transition {transition_id})")
        if self._encryptor is not None:
            try:
                own = self._session.get_key_ratchet(self._self_id)
            except Exception as e:
                self._note(f"own ratchet unavailable: {type(e).__name__}")
                own = None
            if own is not None:
                self._encryptor.set_key_ratchet(own)
                self.own_ratchet = own
                self._note("own ratchet installed for sending")

    def decrypt(self, kind: str, payload: bytes) -> bytes | None:
        if self._decryptor is None or not self.has_ratchet:
            return None
        media = dave.MediaType.video if kind == "video" else dave.MediaType.audio
        return self._decryptor.decrypt(media, payload)

    # Voice channels: every speaker encrypts with their own ratchet, so each
    # speaker needs their own Decryptor (one ratchet each).

    def _install_voice_ratchets(self, transition_id: int) -> None:
        installed = 0
        for user_id in sorted(self._recognized):
            if user_id == self._self_id:
                continue
            if self._install_voice_ratchet(user_id):
                installed += 1
        self.has_ratchet = installed > 0
        self.transitions += 1
        self._note(
            f"voice ratchets installed for {installed} speaker(s) "
            f"(transition {transition_id}, {len(self._recognized) - 1} known)"
        )

    def _install_voice_ratchet(self, user_id: str) -> bool:
        try:
            ratchet = self._session.get_key_ratchet(user_id)
        except Exception as e:
            self._note(f"voice ratchet unavailable for {user_id}: {type(e).__name__}")
            return False
        if ratchet is None:
            return False
        decryptor = self._decryptors.get(user_id)
        if decryptor is None:
            decryptor = self._decryptors[user_id] = dave.Decryptor()
        try:
            decryptor.transition_to_key_ratchet(ratchet)
        except Exception as e:
            self._note(f"voice ratchet for {user_id} rejected: {type(e).__name__}")
            self._decryptors.pop(user_id, None)
            return False
        return True

    def decrypt_for(self, user_id: str, payload: bytes) -> bytes | None:
        """Decrypt an audio frame from a specific speaker."""
        if self._decryptor is not None or not self._session.has_established_group():
            return None
        user_id = str(user_id)
        decryptor = self._decryptors.get(user_id)
        if decryptor is None:
            # A speaker who joined after the last transition: their ratchet is
            # already part of the current epoch, so install it lazily.
            if not self._install_voice_ratchet(user_id):
                return None
            decryptor = self._decryptors.get(user_id)
            if decryptor is None:
                return None
        return decryptor.decrypt(dave.MediaType.audio, payload)

    @property
    def voice_ratchets(self) -> int:
        return len(self._decryptors)
