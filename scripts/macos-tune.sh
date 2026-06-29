#!/usr/bin/env bash
# macOS socket tuning for high-concurrency HTTP load testing.
# Run with: sudo ./scripts/macos-tune.sh

set -euo pipefail

echo "Raising open file descriptor limit for this shell..."
ulimit -n 65536

echo "Applying sysctl socket tuning (requires sudo)..."
sysctl -w net.inet.ip.portrange.first=1024
sysctl -w net.inet.ip.portrange.last=65535
sysctl -w net.inet.tcp.msl=1000
sysctl -w kern.ipc.somaxconn=65536

echo "Done. Current limits:"
ulimit -n
sysctl net.inet.ip.portrange.first net.inet.ip.portrange.last net.inet.tcp.msl kern.ipc.somaxconn
