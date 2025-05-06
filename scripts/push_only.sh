#!/bin/bash
# kitsu-push-only.sh - Push folder directly to Kitsu

FOLDER=$(realpath "$1")
PROJECT="$2"
SEQUENCE="${3:-SQ01}"

docker run -v "$FOLDER":/app/data \
  kitsu-ingest kitsu-ingest --push_only /app/data --push "$PROJECT" --sequence "$SEQUENCE"