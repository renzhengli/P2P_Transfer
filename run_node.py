#!/usr/bin/env python3
"""
Interactive CLI Runner for P2P Node.
Demonstrates:
- Dynamic discovery and tracking of node joins & leaves.
- Pub/Sub messaging across nodes.
- High-speed binary file transfer (.deb, .tar, .bin, etc.) with SHA-256 verification.
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Add python directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent / "python"))

from p2p_network import P2PNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CLI")


async def main():
    parser = argparse.ArgumentParser(
        description="Launch a P2P node with auto-discovery, liveness tracking, and file transfer."
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=52414,
        help="TCP port to listen for Zenoh traffic (default: 52414)",
    )
    parser.add_argument(
        "--discovery-port",
        "-d",
        type=int,
        default=52413,
        help="UDP multicast discovery port (default: 52413)",
    )
    parser.add_argument(
        "--namespace",
        "-n",
        type=str,
        default="my_p2p_network",
        help="Cluster namespace (default: my_p2p_network)",
    )
    parser.add_argument(
        "--id",
        type=str,
        default=None,
        help="Custom 16-hex character Node ID (default: auto-generated)",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default="./downloads",
        help="Directory to store received files (default: ./downloads)",
    )
    parser.add_argument(
        "--send",
        type=str,
        default=None,
        help="Path of file to send immediately after discovering at least one peer",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run without interactive command line prompt (ideal for background services)",
    )

    args = parser.parse_args()

    # Initialize node
    node = P2PNode(
        node_id=args.id,
        namespace=args.namespace,
        listen_port=args.port,
        discovery_port=args.discovery_port,
        download_dir=args.download_dir,
    )

    # Register callbacks
    def on_join(peer_id: str):
        print(f"\n🔔 [CLUSTER UPDATE] New device connected: {peer_id}")
        print(f"📊 Current active devices ({len(node.active_peers)}): {list(node.active_peers)}")

    def on_leave(peer_id: str):
        print(f"\n⚠️  [CLUSTER UPDATE] Device disconnected: {peer_id}")
        print(f"📊 Remaining active devices ({len(node.active_peers)}): {list(node.active_peers)}")

    def on_msg(topic: str, data: bytes):
        try:
            text = data.decode("utf-8")
            print(f"\n💬 [Message on '{topic}'] {text}")
        except Exception:
            print(f"\n💬 [Binary message on '{topic}'] {len(data)} bytes")

    def on_file(info: dict):
        print(
            f"\n🎉 [File Saved] '{info['filename']}' ({info['file_size']} bytes)\n"
            f"   Saved to: {info['saved_to']}\n"
            f"   Speed:    {info['speed_mbps']} MB/s\n"
            f"   SHA256:   {info['sha256']}"
        )

    node.on_peer_join = on_join
    node.on_peer_leave = on_leave
    node.on_message = on_msg
    node.on_file_received = on_file

    # Start node
    await node.start()

    # Subscribe to a general chat topic
    await node.subscribe("chat")

    print("\n" + "=" * 60)
    print(f"🌐 P2P Node Online: {node.node_id}")
    print(f"   Namespace:       {node.namespace}")
    print(f"   Listen Port:     TCP {node.listen_port}")
    print(f"   Discovery Port:  UDP {node.discovery_port}")
    print(f"   Download Folder: {Path(args.download_dir).resolve()}")
    print("=" * 60)

    # If --send was provided, wait for a peer and send
    if args.send:
        file_to_send = Path(args.send)
        if not file_to_send.is_file():
            print(f"❌ Error: file '{args.send}' does not exist.")
        else:
            print(f"⏳ Waiting for at least one peer before sending '{file_to_send.name}'...")
            while not node.active_peers:
                await asyncio.sleep(0.5)
            print(f"🚀 Found peer! Sending '{file_to_send.name}' now...")
            await node.send_file(file_to_send)

    if args.non_interactive:
        print("Running in non-interactive mode. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await node.stop()
        return

    # Interactive prompt
    print("\nInteractive Commands:")
    print("  nodes                        - Show active nodes in cluster")
    print("  send <file_path>             - Broadcast file (.deb, .tar, etc.) to ALL nodes")
    print("  send <file_path> <peer_id>   - Send file ONLY to a specific node")
    print("  msg <text>                   - Broadcast a message on 'chat' topic")
    print("  quit / exit                  - Exit node\n")

    loop = asyncio.get_running_loop()

    try:
        while True:
            cmd = await loop.run_in_executor(None, input, "p2p> ")
            cmd = cmd.strip()
            if not cmd:
                continue

            parts = cmd.split(maxsplit=1)
            action = parts[0].lower()

            if action in ("quit", "exit"):
                break
            elif action == "nodes":
                print(f"Active devices ({len(node.active_peers)}):")
                for p in node.active_peers:
                    print(f"  • {p}")
                if not node.active_peers:
                    print("  (No other devices discovered yet)")
            elif action == "send":
                if len(parts) < 2:
                    print("Usage: send <path_to_file> [target_peer_id]")
                    print("  - send <path>               : Broadcast to ALL connected devices")
                    print("  - send <path> <target_peer> : Send ONLY to the specified device")
                    continue
                cmd_args = parts[1].strip().split()
                fpath = Path(cmd_args[0].strip("'\""))
                target_peer = cmd_args[1].strip() if len(cmd_args) > 1 else None

                if not fpath.is_file():
                    print(f"❌ File not found: {fpath}")
                    continue
                try:
                    if target_peer:
                        print(f"🎯 Sending '{fpath.name}' targeting specific device: {target_peer}...")
                    else:
                        print(f"📢 Broadcasting '{fpath.name}' to ALL {len(node.active_peers)} connected devices...")
                    await node.send_file(fpath, target_peer=target_peer)
                except Exception as e:
                    print(f"Send failed: {e}")
            elif action == "msg":
                if len(parts) < 2:
                    print("Usage: msg <text>")
                    continue
                text = parts[1].encode("utf-8")
                await node.publish("chat", text)
            else:
                print(f"Unknown command: '{action}'. Commands: nodes, send <path> [peer_id], msg <text>, quit")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
