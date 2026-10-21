#!/usr/bin/env python3
"""
Automated End-to-End Verification Test for my_p2p_network.
Tests:
1. Multi-node automatic discovery.
2. Binary package transfer (.tar / .deb) with SHA-256 verification.
3. Automatic node disconnection/eviction detection when a node leaves.
"""

import asyncio
import hashlib
import logging
import os
import shutil
import sys
import tarfile
from pathlib import Path

# Add python directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent / "python"))

from p2p_network import P2PNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Test")


def create_test_tar_archive(target_path: Path, size_mb: int = 2) -> str:
    """Creates a sample .tar file with dummy data and returns its SHA-256."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = target_path.parent / "_temp_tar_content"
    temp_dir.mkdir(parents=True, exist_ok=True)

    dummy_file = temp_dir / "package.bin"
    chunk = os.urandom(1024 * 1024)
    with open(dummy_file, "wb") as f:
        for _ in range(size_mb):
            f.write(chunk)

    with tarfile.open(target_path, "w") as tar:
        tar.add(dummy_file, arcname="package.bin")

    shutil.rmtree(temp_dir)

    hasher = hashlib.sha256()
    with open(target_path, "rb") as f:
        while b := f.read(1024 * 1024):
            hasher.update(b)
    return hasher.hexdigest()


async def run_test():
    test_dir = Path(__file__).resolve().parent / "test_scratch"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True, exist_ok=True)

    download_dir_b = test_dir / "node_b_downloads"
    download_dir_b.mkdir(parents=True, exist_ok=True)

    # 1. Create a test .tar package (simulating a .tar or .deb archive)
    test_tar = test_dir / "sample_release_v1.tar"
    expected_sha256 = create_test_tar_archive(test_tar, size_mb=2)
    logger.info(f"Generated test archive: {test_tar} ({test_tar.stat().st_size} bytes)")
    logger.info(f"Expected SHA-256: {expected_sha256}")

    # 2. Instantiate Node A and Node B
    # Node A listens on 52414, Node B listens on 52424. Both use UDP 52413 for discovery.
    node_a = P2PNode(
        namespace="test_cluster",
        listen_port=52414,
        discovery_port=52413,
        download_dir=test_dir / "node_a_downloads",
    )
    node_b = P2PNode(
        namespace="test_cluster",
        listen_port=52424,
        discovery_port=52413,
        download_dir=download_dir_b,
    )

    discovered_event_a = asyncio.Event()
    discovered_event_b = asyncio.Event()
    file_received_event_b = asyncio.Event()
    received_file_info = {}
    node_left_event_a = asyncio.Event()

    node_a.on_peer_join = lambda peer: discovered_event_a.set()
    node_b.on_peer_join = lambda peer: discovered_event_b.set()

    def on_file_received_b(info):
        nonlocal received_file_info
        received_file_info = info
        file_received_event_b.set()

    node_b.on_file_received = on_file_received_b
    node_a.on_peer_leave = lambda peer: node_left_event_a.set()

    logger.info("Starting Node A...")
    await node_a.start()

    logger.info("Starting Node B...")
    await node_b.start()

    # 3. Test Peer Discovery
    logger.info("Waiting for mutual discovery between Node A and Node B...")
    try:
        await asyncio.wait_for(
            asyncio.gather(discovered_event_a.wait(), discovered_event_b.wait()),
            timeout=10.0,
        )
        logger.info(f" Mutual discovery success! Node A peers: {node_a.active_peers}, Node B peers: {node_b.active_peers}")
    except asyncio.TimeoutError:
        logger.error(f"❌ Discovery timeout! A peers: {node_a.active_peers}, B peers: {node_b.active_peers}")
        await node_a.stop()
        await node_b.stop()
        sys.exit(1)

    # 4. Test File Transfer (Node A -> Node B)
    logger.info("Sending sample_release_v1.tar from Node A to Node B...")
    await node_a.send_file(test_tar)

    logger.info("Waiting for Node B to receive and verify file...")
    try:
        await asyncio.wait_for(file_received_event_b.wait(), timeout=15.0)
        logger.info(f"🎉 Node B received file! Details: {received_file_info}")
        assert received_file_info["sha256"] == expected_sha256, "SHA256 mismatch!"
        assert Path(received_file_info["saved_to"]).exists(), "Saved file does not exist!"
        logger.info(" File transfer and integrity verification passed!")
    except asyncio.TimeoutError:
        logger.error("❌ File transfer timed out!")
        await node_a.stop()
        await node_b.stop()
        sys.exit(1)

    # 5. Test Node Disconnection / Eviction Detection
    logger.info("Stopping Node B to test disconnection detection on Node A...")
    await node_b.stop()

    logger.info("Waiting for Node A to detect Node B's exit...")
    try:
        await asyncio.wait_for(node_left_event_a.wait(), timeout=10.0)
        logger.info(f" Node A successfully detected Node B left! Remaining peers: {node_a.active_peers}")
    except asyncio.TimeoutError:
        logger.error("❌ Node disconnection detection timed out!")
        await node_a.stop()
        sys.exit(1)

    # Clean up
    await node_a.stop()
    if test_dir.exists():
        shutil.rmtree(test_dir)
    print("\n" + "=" * 60)
    print(" ALL TESTS PASSED SUCCESSFULLY! (Discovery, File Transfer, Leave Detection)")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(run_test())
