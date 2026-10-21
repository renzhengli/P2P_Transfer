import asyncio
import logging
import os
import secrets
from collections.abc import Callable
from pathlib import Path

# Attempt importing p2p_rs from current package or system path
try:
    from .p2p_rs import FromSwarm, NetworkingHandle
except ImportError:
    try:
        from p2p_rs import FromSwarm, NetworkingHandle
    except ImportError:
        import sys
        # Look in target/release
        rel_path = Path(__file__).resolve().parent.parent.parent / "target" / "release"
        if rel_path.exists():
            sys.path.insert(0, str(rel_path))
        from p2p_rs import FromSwarm, NetworkingHandle

from .file_transfer import (
    DEFAULT_CHUNK_SIZE,
    TOPIC_FILE_CHUNK,
    TOPIC_FILE_META,
    FileTransferManager,
)

logger = logging.getLogger("p2p_network.node")


def generate_node_id() -> str:
    """Generates a valid Zenoh identity (16-char hex, non-zero starting character)."""
    h = secrets.token_hex(8)
    if h[0] == "0":
        h = "1" + h[1:]
    return h


class P2PNode:
    """
    High-level P2P Node supporting:
    - Zero-config automatic peer discovery via UDP Multicast (IPv6)
    - Peer liveness tracking (detect join & leave events in real-time)
    - Distributed GossipSub pub/sub messaging
    - High-performance chunked file transfer (.deb, .tar, .bin, etc.)
    """

    def __init__(
        self,
        node_id: str | None = None,
        namespace: str = "my_p2p_network",
        listen_port: int = 52414,
        discovery_port: int = 52413,
        download_dir: str | Path = "./downloads",
    ):
        self.node_id: str = node_id or generate_node_id()
        self.namespace: str = namespace
        self.listen_port: int = listen_port
        self.discovery_port: int = discovery_port

        self._handle: NetworkingHandle | None = None
        self._running: bool = False
        self._recv_task: asyncio.Task | None = None
        self.active_peers: set[str] = set()
        self.subscribed_topics: set[str] = set()

        # Callbacks
        self.on_peer_join: Callable[[str], None] | None = None
        self.on_peer_leave: Callable[[str], None] | None = None
        self.on_message: Callable[[str, bytes], None] | None = None
        self.on_file_received: Callable[[dict], None] | None = None

        # File transfer manager
        self.file_transfer = FileTransferManager(
            node_id=self.node_id,
            publish_func=self.publish,
            download_dir=download_dir,
        )
        self.file_transfer.on_file_received_callback = self._on_internal_file_received

    def _on_internal_file_received(self, info: dict):
        if self.on_file_received:
            try:
                self.on_file_received(info)
            except Exception as e:
                logger.error(f"Error in on_file_received user callback: {e}")

    async def start(self) -> None:
        """
        Starts the P2P node and initializes discovery & networking.
        """
        if self._running:
            return

        logger.info(
            f"🚀 Starting P2P Node [ID: {self.node_id}] "
            f"namespace='{self.namespace}' listen_port={self.listen_port} discovery_port={self.discovery_port}"
        )

        self._handle = NetworkingHandle.new(
            self.node_id,
            self.namespace,
            self.listen_port,
            self.discovery_port,
        )
        self._running = True

        # Launch background receive loop first so the swarm stream begins polling
        self._recv_task = asyncio.create_task(self._recv_loop())

        # Subscribe to file transfer topics automatically
        await self.subscribe(TOPIC_FILE_META)
        await self.subscribe(TOPIC_FILE_CHUNK)

        logger.info(f"✅ Node '{self.node_id}' is now online and discovering peers.")

    async def stop(self) -> None:
        """
        Stops the P2P node gracefully.
        """
        if not self._running:
            return

        logger.info(f"🛑 Stopping P2P Node '{self.node_id}'...")
        self._running = False
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None

        self.active_peers.clear()
        self._handle = None
        logger.info(f"Node '{self.node_id}' stopped.")

    async def subscribe(self, topic: str) -> bool:
        """Subscribes to a pub/sub topic."""
        if not self._handle:
            raise RuntimeError("Node is not running.")
        res = await self._handle.gossipsub_subscribe(topic)
        self.subscribed_topics.add(topic)
        logger.debug(f"Subscribed to topic: {topic}")
        return res

    async def unsubscribe(self, topic: str) -> bool:
        """Unsubscribes from a pub/sub topic."""
        if not self._handle:
            raise RuntimeError("Node is not running.")
        res = await self._handle.gossipsub_unsubscribe(topic)
        self.subscribed_topics.discard(topic)
        logger.debug(f"Unsubscribed from topic: {topic}")
        return res

    async def publish(self, topic: str, data: bytes) -> None:
        """Publishes binary data to a topic across the cluster."""
        if not self._handle:
            raise RuntimeError("Node is not running.")
        await self._handle.gossipsub_publish(topic, data)

    async def send_file(
        self,
        filepath: str | Path,
        target_peer: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> dict:
        """
        Transfers a file (.deb, .tar, etc.) to all peers or a specific peer.
        """
        return await self.file_transfer.send_file(
            filepath=filepath,
            target_peer=target_peer,
            progress_callback=progress_callback,
        )

    async def _recv_loop(self) -> None:
        while self._running:
            try:
                event = await self._handle.recv()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.error(f"Error in recv loop: {e}")
                    await asyncio.sleep(0.5)
                continue

            if event.is_connection:
                connected = event.connected
                peer_id = event.peer_id
                # Filter out events about our own node
                if not peer_id or peer_id == self.node_id:
                    continue

                if connected:
                    if peer_id not in self.active_peers:
                        self.active_peers.add(peer_id)
                        logger.info(
                            f"🤝 [Peer Discovered] Node joined: '{peer_id}' "
                            f"(Total active peers: {len(self.active_peers)})"
                        )
                        if self.on_peer_join:
                            try:
                                self.on_peer_join(peer_id)
                            except Exception as ex:
                                logger.error(f"Error in on_peer_join callback: {ex}")
                else:
                    if peer_id in self.active_peers:
                        self.active_peers.remove(peer_id)
                        logger.info(
                            f"👋 [Peer Disconnected] Node left: '{peer_id}' "
                            f"(Total active peers: {len(self.active_peers)})"
                        )
                        if self.on_peer_leave:
                            try:
                                self.on_peer_leave(peer_id)
                            except Exception as ex:
                                logger.error(f"Error in on_peer_leave callback: {ex}")

            elif event.is_message:
                topic = event.topic
                data = bytes(event.data)

                # Route file transfer messages
                handled = await self.file_transfer.handle_message(topic, data)
                if not handled and self.on_message:
                    try:
                        self.on_message(topic, data)
                    except Exception as ex:
                        logger.error(f"Error in on_message callback: {ex}")
