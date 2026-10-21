import asyncio
import hashlib
import json
import logging
import os
import struct
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger("p2p_network.file_transfer")

DEFAULT_CHUNK_SIZE = 128 * 1024  # 128 KB
TOPIC_FILE_META = "p2p_file_meta"
TOPIC_FILE_CHUNK = "p2p_file_chunk"


class FileTransferManager:
    """
    Manages chunked P2P file transfers over Zenoh pub/sub topics.
    Supports arbitrary file formats (.deb, .tar, .bin, etc.) with SHA-256 integrity checks.
    """

    def __init__(
        self,
        node_id: str,
        publish_func: Callable[[str, bytes], Any],
        download_dir: str | Path = "./downloads",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.node_id = node_id
        self.publish_func = publish_func
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size

        # transfer_id -> incoming transfer state
        self.incoming_transfers: dict[str, dict] = {}

        # Callbacks
        self.on_file_received_callback: Callable[[dict], None] | None = None
        self.on_progress_callback: Callable[[str, int, int], None] | None = None

    async def handle_message(self, topic: str, data: bytes) -> bool:
        """
        Processes incoming file transfer messages. Returns True if message was handled.
        """
        if topic == TOPIC_FILE_META:
            await self._handle_meta(data)
            return True
        elif topic == TOPIC_FILE_CHUNK:
            await self._handle_chunk(data)
            return True
        return False

    async def _handle_meta(self, data: bytes) -> None:
        try:
            meta = json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to parse file metadata: {e}")
            return

        transfer_id = meta.get("transfer_id")
        sender_id = meta.get("sender_id")
        target_id = meta.get("target_id")
        filename = meta.get("filename")
        file_size = meta.get("file_size")
        total_chunks = meta.get("total_chunks")
        expected_sha256 = meta.get("sha256")

        # Ignore transfers initiated by ourselves
        if sender_id == self.node_id:
            return

        # If a specific target was specified and it's not us, ignore
        if target_id is not None and target_id != self.node_id:
            return

        logger.info(
            f"📥 [Incoming File] '{filename}' ({file_size / (1024 * 1024):.2f} MB) "
            f"from node '{sender_id}'. Total chunks: {total_chunks}"
        )

        part_file_path = self.download_dir / f"{filename}.{transfer_id}.part"

        # Initialize transfer state
        try:
            f = open(part_file_path, "wb")
            # Pre-allocate or truncate file
            f.truncate(file_size)
        except OSError as e:
            logger.error(f"Failed to create part file {part_file_path}: {e}")
            return

        self.incoming_transfers[transfer_id] = {
            "meta": meta,
            "file": f,
            "part_path": part_file_path,
            "final_path": self.download_dir / filename,
            "received_chunks": set(),
            "total_chunks": total_chunks,
            "file_size": file_size,
            "expected_sha256": expected_sha256,
            "start_time": time.time(),
        }

    async def _handle_chunk(self, data: bytes) -> None:
        if len(data) < 4:
            return

        (header_len,) = struct.unpack("!I", data[:4])
        if len(data) < 4 + header_len:
            return

        header_bytes = data[4 : 4 + header_len]
        chunk_data = data[4 + header_len :]

        try:
            header = json.loads(header_bytes.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to parse chunk header: {e}")
            return

        transfer_id = header.get("transfer_id")
        chunk_idx = header.get("chunk_idx")

        transfer = self.incoming_transfers.get(transfer_id)
        if not transfer:
            return

        if chunk_idx in transfer["received_chunks"]:
            return  # Duplicate chunk

        file_obj = transfer["file"]
        chunk_size = transfer["meta"].get("chunk_size", self.chunk_size)
        offset = chunk_idx * chunk_size

        try:
            file_obj.seek(offset)
            file_obj.write(chunk_data)
            transfer["received_chunks"].add(chunk_idx)
        except Exception as e:
            logger.error(f"Failed to write chunk {chunk_idx}: {e}")
            return

        received_count = len(transfer["received_chunks"])
        total = transfer["total_chunks"]

        if self.on_progress_callback:
            try:
                self.on_progress_callback(transfer_id, received_count, total)
            except Exception:
                pass

        if received_count % max(1, total // 5) == 0 or received_count == total:
            pct = (received_count / total) * 100
            logger.info(
                f"⏳ [Downloading] '{transfer['meta']['filename']}': "
                f"{received_count}/{total} chunks ({pct:.1f}%)"
            )

        if received_count >= total:
            await self._finalize_transfer(transfer_id)

    async def _finalize_transfer(self, transfer_id: str) -> None:
        transfer = self.incoming_transfers.pop(transfer_id, None)
        if not transfer:
            return

        transfer["file"].close()
        part_path: Path = transfer["part_path"]
        final_path: Path = transfer["final_path"]
        expected_sha256 = transfer["expected_sha256"]

        # Verify SHA-256 hash
        hasher = hashlib.sha256()
        with open(part_path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                hasher.update(chunk)
        calculated_sha256 = hasher.hexdigest()

        if calculated_sha256.lower() != expected_sha256.lower():
            logger.error(
                f"❌ [File Integrity Failed] SHA-256 mismatch for '{transfer['meta']['filename']}'!\n"
                f"Expected: {expected_sha256}\nGot:      {calculated_sha256}"
            )
            part_path.unlink(missing_ok=True)
            return

        # Avoid overwriting existing files with the same name
        target_file = final_path
        if target_file.exists():
            stem = final_path.stem
            suffix = final_path.suffix
            idx = 1
            while target_file.exists():
                target_file = self.download_dir / f"{stem}_{idx}{suffix}"
                idx += 1

        part_path.rename(target_file)
        elapsed = time.time() - transfer["start_time"]
        file_size_mb = transfer["file_size"] / (1024 * 1024)
        speed = file_size_mb / max(0.001, elapsed)

        result_info = {
            "transfer_id": transfer_id,
            "filename": transfer["meta"]["filename"],
            "saved_to": str(target_file),
            "file_size": transfer["file_size"],
            "sha256": calculated_sha256,
            "sender_id": transfer["meta"]["sender_id"],
            "elapsed_seconds": round(elapsed, 2),
            "speed_mbps": round(speed, 2),
        }

        logger.info(
            f"✅ [File Received Successfully] '{target_file.name}' "
            f"({file_size_mb:.2f} MB, {speed:.2f} MB/s) saved to {target_file}"
        )

        if self.on_file_received_callback:
            try:
                self.on_file_received_callback(result_info)
            except Exception as e:
                logger.error(f"Error in on_file_received callback: {e}")

    async def send_file(
        self,
        filepath: str | Path,
        target_peer: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> dict:
        """
        Sends a file to the cluster (or to a specific peer) in chunks.
        """
        path = Path(filepath)
        if not path.is_file():
            raise FileNotFoundError(f"File '{filepath}' does not exist or is not a file.")

        file_size = path.stat().st_size
        filename = path.name

        # 1. Compute SHA-256
        logger.info(f"Computing SHA-256 for '{filename}' ({file_size / (1024 * 1024):.2f} MB)...")
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while block := f.read(1024 * 1024):
                hasher.update(block)
        file_sha256 = hasher.hexdigest()

        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size if file_size > 0 else 1
        transfer_id = str(uuid.uuid4())[:8]

        meta = {
            "transfer_id": transfer_id,
            "filename": filename,
            "file_size": file_size,
            "total_chunks": total_chunks,
            "chunk_size": self.chunk_size,
            "sha256": file_sha256,
            "sender_id": self.node_id,
            "target_id": target_peer,
        }

        logger.info(
            f"🚀 [Sending File] '{filename}' ({file_size / (1024 * 1024):.2f} MB) "
            f"in {total_chunks} chunks (Transfer ID: {transfer_id})"
        )

        # 2. Publish metadata
        meta_payload = json.dumps(meta).encode("utf-8")
        await self.publish_func(TOPIC_FILE_META, meta_payload)

        # Small pause so receivers can open the part file
        await asyncio.sleep(0.05)

        # 3. Publish chunks
        sent_chunks = 0
        with open(path, "rb") as f:
            for chunk_idx in range(total_chunks):
                chunk_bytes = f.read(self.chunk_size)
                header_json = json.dumps({
                    "transfer_id": transfer_id,
                    "chunk_idx": chunk_idx,
                }).encode("utf-8")

                packet = struct.pack("!I", len(header_json)) + header_json + chunk_bytes
                await self.publish_func(TOPIC_FILE_CHUNK, packet)

                sent_chunks += 1
                if progress_callback:
                    try:
                        progress_callback(sent_chunks, total_chunks)
                    except Exception:
                        pass

                if sent_chunks % max(1, total_chunks // 5) == 0 or sent_chunks == total_chunks:
                    pct = (sent_chunks / total_chunks) * 100
                    logger.info(f"📤 [Uploading] '{filename}': {sent_chunks}/{total_chunks} chunks ({pct:.1f}%)")

                # Minimal yield to keep event loop and network responsive
                if sent_chunks % 8 == 0:
                    await asyncio.sleep(0.005)

        logger.info(f"✨ [File Sent Finished] '{filename}' ({file_size} bytes)")
        return meta
