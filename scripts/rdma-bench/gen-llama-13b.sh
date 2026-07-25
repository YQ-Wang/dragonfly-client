#!/usr/bin/env bash
# Materializes the exact file layout and byte sizes of meta-llama/Llama-2-13b-chat-hf in a memory
# filesystem. The S3 copy of the real weights is unreachable from this cluster, and RDMA goodput
# does not depend on the byte values, so the shards are filled from a random 1 GiB pattern.
set -euo pipefail

DIR=${1:-/mnt/memfs/llama-2-13b-chat-hf}
BLOCK=/mnt/memfs/.pattern

# Exact sizes from the upstream repository.
FILES="
config.json:587
generation_config.json:188
model-00001-of-00003.safetensors:9948728430
model-00002-of-00003.safetensors:9904165024
model-00003-of-00003.safetensors:6178961232
model.safetensors.index.json:33444
special_tokens_map.json:414
tokenizer.json:1842767
tokenizer.model:499723
tokenizer_config.json:1618
"

rm -rf "$DIR"
mkdir -p "$DIR"

if [ ! -f "$BLOCK" ]; then
  head -c 1073741824 /dev/urandom > "$BLOCK"
fi

for entry in $FILES; do
  name=${entry%%:*}
  size=${entry##*:}
  path="$DIR/$name"
  full=$((size / 1073741824))
  for _ in $(seq 0 $full); do
    cat "$BLOCK" >> "$path"
  done
  truncate -s "$size" "$path"
done

total=$(du -sb "$DIR" | cut -f1)
echo "generated $(ls -1 "$DIR" | wc -l) files, $total bytes ($((total / 1024 / 1024 / 1024)) GiB) in $DIR"
ls -la "$DIR"
