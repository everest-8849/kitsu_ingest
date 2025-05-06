#!/bin/bash
# kitsu-csv.sh - Process a CSV file

# Check if CSV path is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 CSV_PATH [SEQUENCE]"
    exit 1
fi

# Get arguments
CSV_PATH="$1"
SEQUENCE="${2:-SQ01}"

# Get directory paths
CSV_DIR=$(dirname "$CSV_PATH")
CSV_FILE=$(basename "$CSV_PATH")

# Create output directory with proper permissions
OUTPUT_DIR="$(pwd)/processed"
mkdir -p "$OUTPUT_DIR"
chmod -R 777 "$OUTPUT_DIR"

echo "Processing CSV file with sequence: $SEQUENCE"

# Run the container with root user to avoid permission issues
docker run --rm \
  -v "$CSV_DIR":/app/data_csv \
  -v "$OUTPUT_DIR":/app/kitsu_ingest/processed \
  --user root \
  kitsu-ingest kitsu-ingest --csv "/app/data_csv/$CSV_FILE" --sequence "$SEQUENCE"

# Fix permissions after container runs
chmod -R 777 "$OUTPUT_DIR"