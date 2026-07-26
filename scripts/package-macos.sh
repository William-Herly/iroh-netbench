#!/usr/bin/env bash
set -euo pipefail

workspace_dir="$(cd "$(dirname "$0")/.." && pwd)"
dist_dir="$workspace_dir/dist/transfer-demo/macos"
app_dir="$dist_dir/Iroh Transfer Demo.app"
client_config="$workspace_dir/private-pki/private-infra.json"
client_ca="$workspace_dir/private-pki/ca.crt"

test -f "$client_config"
test -f "$client_ca"

cargo build \
  --manifest-path "$workspace_dir/Cargo.toml" \
  --release \
  --target aarch64-apple-darwin \
  -p transfer-demo \
  --bin transfer-demo

mkdir -p "$app_dir/Contents/MacOS" "$app_dir/Contents/Resources"
cp \
  "$workspace_dir/target/aarch64-apple-darwin/release/transfer-demo" \
  "$app_dir/Contents/MacOS/transfer-demo"
cp "$workspace_dir/packaging/macos/Info.plist" "$app_dir/Contents/Info.plist"
cp "$workspace_dir/assets/fonts/OFL.txt" \
  "$app_dir/Contents/Resources/Noto-Sans-CJK-OFL.txt"
cp "$client_config" "$app_dir/Contents/Resources/private-infra.json"
cp "$client_ca" "$app_dir/Contents/Resources/private-ca.crt"

codesign --force --deep --sign - "$app_dir"
touch "$app_dir"
ditto -c -k --sequesterRsrc --keepParent "$app_dir" \
  "$dist_dir/iroh-transfer-demo-macos-arm64.zip"

echo "$app_dir"
echo "$dist_dir/iroh-transfer-demo-macos-arm64.zip"
