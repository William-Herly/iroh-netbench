#!/usr/bin/env bash
set -euo pipefail

workspace_dir="$(cd "$(dirname "$0")/.." && pwd)"
dist_dir="$workspace_dir/dist/transfer-demo/windows-x86_64"
client_config="$workspace_dir/private-pki/private-infra.json"
client_ca="$workspace_dir/private-pki/ca.crt"

test -f "$client_config"
test -f "$client_ca"

cargo zigbuild \
  --manifest-path "$workspace_dir/Cargo.toml" \
  --release \
  --target x86_64-pc-windows-gnu \
  -p transfer-demo \
  --bin transfer-demo

mkdir -p "$dist_dir"
cp "$workspace_dir/target/x86_64-pc-windows-gnu/release/transfer-demo.exe" \
  "$dist_dir/iroh-transfer-demo.exe"
cp "$workspace_dir/assets/fonts/OFL.txt" \
  "$dist_dir/Noto-Sans-CJK-OFL.txt"
cp "$client_config" "$dist_dir/private-infra.json"
cp "$client_ca" "$dist_dir/private-ca.crt"

cd "$dist_dir"
zip -q -9 iroh-transfer-demo-windows-x86_64.zip \
  iroh-transfer-demo.exe Noto-Sans-CJK-OFL.txt private-infra.json private-ca.crt
echo "$dist_dir/iroh-transfer-demo.exe"
echo "$dist_dir/iroh-transfer-demo-windows-x86_64.zip"
