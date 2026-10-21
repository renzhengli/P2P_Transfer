"""
P2P Network Package:
Decoupled multi-node auto-discovery, cluster liveness tracking, and chunked file transfer.
"""

from .file_transfer import FileTransferManager
from .node import P2PNode, generate_node_id

try:
    from .p2p_rs import FromSwarm, NetworkingHandle
except ImportError:
    pass

__all__ = [
    "P2PNode",
    "FileTransferManager",
    "generate_node_id",
    "NetworkingHandle",
    "FromSwarm",
]
