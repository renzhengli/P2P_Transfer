#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  Building P2P Network Rust Module (p2p_rs)"
echo "=========================================="

# 1. Check Cargo
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: cargo is not installed."
    echo "   Please install Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# 2. Build Rust release library
echo "==> Compiling Rust crate in release mode (cargo build --release)..."
cargo build --release -p p2p_rs

# 3. Locate compiled library (.so on Linux, .dylib on macOS)
if [ -f "target/release/libp2p_rs.so" ]; then
    SO_FILE="target/release/libp2p_rs.so"
elif [ -f "target/release/libp2p_rs.dylib" ]; then
    SO_FILE="target/release/libp2p_rs.dylib"
else
    echo "❌ Build failed: output library not found in target/release/"
    exit 1
fi

# 4. Copy to python module directory
DEST_DIR="python/p2p_network"
mkdir -p "$DEST_DIR"
cp -f "$SO_FILE" "$DEST_DIR/p2p_rs.so"

echo "=========================================="
echo "✅ Build completed successfully!"
echo "   Output: $DEST_DIR/p2p_rs.so"
echo "   You can now run: python3 run_node.py"
echo "=========================================="
