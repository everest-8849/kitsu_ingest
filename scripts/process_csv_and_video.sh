#!/bin/bash
# kitsu-video.sh - Process a CSV and video file

# Check arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 CSV_PATH VIDEO_PATH [SEQUENCE]"
    exit 1
fi

# Get arguments
CSV_PATH="$1"
VIDEO_PATH="$2"
SEQUENCE="${3:-SQ01}"

# Get directory paths
CSV_DIR=$(dirname "$CSV_PATH")
CSV_FILE=$(basename "$CSV_PATH")
VIDEO_DIR=$(dirname "$VIDEO_PATH")
VIDEO_FILE=$(basename "$VIDEO_PATH")

# Create output directory with proper permissions
OUTPUT_DIR="$(pwd)/processed"
mkdir -p "$OUTPUT_DIR"
chmod -R 777 "$OUTPUT_DIR"

echo "Processing CSV and video with sequence: $SEQUENCE"

# Run the container with root user to avoid permission issues
docker run --rm \
  -v "$CSV_DIR":/app/data_csv \
  -v "$VIDEO_DIR":/app/data_video \
  -v "$OUTPUT_DIR":/app/kitsu_ingest/processed \
  --user root \
  kitsu-ingest kitsu-ingest --csv "/app/data_csv/$CSV_FILE" \
  --video "/app/data_video/$VIDEO_FILE" --sequence "$SEQUENCE"

# Fix permissions after container runs
chmod -R 777 "$OUTPUT_DIR"