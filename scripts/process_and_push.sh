#!/bin/bash
# kitsu-push.sh - Process and push to Kitsu

# Get absolute paths
CSV_PATH=$(realpath "$1")
CSV_DIR=$(dirname "$CSV_PATH")
CSV_FILE=$(basename "$CSV_PATH")

VIDEO_PATH=$(realpath "$2")
VIDEO_DIR=$(dirname "$VIDEO_PATH")
VIDEO_FILE=$(basename "$VIDEO_PATH")

PROJECT="$3"
SEQUENCE="${4:-SQ01}"

docker run -v "$CSV_DIR":/app/data_csv -v "$VIDEO_DIR":/app/data_video \
  -v "$(pwd)/processed":/app/kitsu_ingest/processed \
  kitsu-ingest kitsu-ingest --csv /app/data_csv/"$CSV_FILE" \
  --video /app/data_video/"$VIDEO_FILE" --push "$PROJECT" --sequence "$SEQUENCE"