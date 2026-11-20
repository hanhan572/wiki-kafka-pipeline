#!/usr/bin/env bash
# Publish the all-in-one image to a Hugging Face Space.
#   usage: scripts/deploy_hf.sh <hf-username>/<space-name>
# You will be asked for your Hugging Face username and an access token with WRITE permission as the password.
set -euo pipefail
SPACE="${1:?usage: scripts/deploy_hf.sh <hf-username>/<space-name>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
cp -R "$ROOT/pipeline" "$ROOT/dashboard" "$ROOT/requirements.txt" "$TMP/"
cp "$ROOT/deploy/huggingface/"{Dockerfile,start.sh,server.properties} "$TMP/"
cp "$ROOT/deploy/huggingface/SPACE_README.md" "$TMP/README.md"
find "$TMP" -name __pycache__ -prune -exec rm -rf {} +
cd "$TMP"
git init -q -b main
git add -A
git -c user.name="deploy" -c user.email="deploy@example.com" commit -q -m "Deploy Kafka pipeline Space"
git push --force "https://huggingface.co/spaces/${SPACE}" main
echo "Pushed. Add the DATABASE_URL secret in the Space settings, then open https://huggingface.co/spaces/${SPACE}"
