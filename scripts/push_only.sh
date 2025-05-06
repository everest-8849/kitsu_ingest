#!/bin/bash
# kitsu-push-only.sh - Push folder directly to Kitsu

# Check arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 FOLDER_PATH PROJECT [SEQUENCE]"
    exit 1
fi

# Get arguments
FOLDER_PATH="$1"
PROJECT="$2"
SEQUENCE="${3:-SQ01}"

# Use realpath to get absolute path
FOLDER=$(realpath "$FOLDER_PATH")

# Create output directory with proper permissions
OUTPUT_DIR="$(pwd)/processed"
mkdir -p "$OUTPUT_DIR"
chmod -R 777 "$OUTPUT_DIR"

echo "Pushing folder to Kitsu with project: $PROJECT, sequence: $SEQUENCE"

# Run the container with root user to avoid permission issues
docker run --rm \
  -v "$FOLDER":/app/data \
  -v "$OUTPUT_DIR":/app/kitsu_ingest/processed \
  --user root \
  kitsu-ingest kitsu-ingest --push_only /app/data --push "$PROJECT" --sequence "$SEQUENCE"

# Fix permissions after container runs
chmod -R 777 "$OUTPUT_DIR"