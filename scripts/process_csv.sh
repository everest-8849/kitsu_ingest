#!/bin/bash
# kitsu-csv.sh - Process a CSV file

# Get absolute path
CSV_PATH=$(realpath "$1")
CSV_DIR=$(dirname "$CSV_PATH")
CSV_FILE=$(basename "$CSV_PATH")

docker run -v "$CSV_DIR":/app/data -v "$(pwd)/processed":/app/kitsu_ingest/processed \
  kitsu-ingest kitsu-ingest --csv /app/data/"$CSV_FILE" --sequence "${2:-SQ01}"