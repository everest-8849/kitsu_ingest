                 # Kitsu Ingest Docker Scripts Documentation

## Overview

This documentation covers the Docker scripts used to process and upload data to Kitsu. Each script is designed for a specific workflow.

## Prerequisites

- Docker installed and running
- use `build_docker.sh` to build Docker image
- Appropriate permissions to execute scripts and access files

## Scripts

### 1. `process_csv.sh`

Processes a CSV file containing shot data.

#### Usage
```bash
./scripts/process_csv.sh CSV_PATH [SEQUENCE]
```

#### Parameters
- `CSV_PATH`: Path to the CSV file
- `SEQUENCE`: (Optional) Sequence identifier (default: SQ01)

#### Example
```bash
./scripts/process_csv.sh ~/data/shots.csv SQ02
```

---

### 2. `process_csv_and_video.sh`

Processes a CSV file along with a video file to generate shots.

#### Usage
```bash
./scripts/process_csv_and_video.sh CSV_PATH VIDEO_PATH [SEQUENCE]
```

#### Parameters
- `CSV_PATH`: Path to the CSV file
- `VIDEO_PATH`: Path to the video file
- `SEQUENCE`: (Optional) Sequence identifier (default: SQ01)

#### Example
```bash
./scripts/process_csv_and_video.sh ~/data/shots.csv ~/videos/footage.mp4 SQ02
```

---

### 3. `push_only.sh`

Pushes an existing folder of processed files directly to Kitsu.

#### Usage
```bash
./scripts/push_only.sh FOLDER_PATH PROJECT [SEQUENCE]
```

#### Parameters
- `FOLDER_PATH`: Path to the folder containing files to push
- `PROJECT`: Target Kitsu project identifier
- `SEQUENCE`: (Optional) Sequence identifier (default: SQ01)

#### Example
```bash
./scripts/push_only.sh ~/renders/project123 PROJECT_X SQ02
```

---

### 4. `process_and_push.sh`

Processes data and pushes it to Kitsu in a single operation.

#### Usage
```bash
./scripts/process_and_push.sh CSV_PATH VIDEO_PATH PROJECT [SEQUENCE]
```

#### Parameters
- `CSV_PATH`: Path to the CSV file
- `VIDEO_PATH`: Path to the video file
- `PROJECT`: Target Kitsu project identifier
- `SEQUENCE`: (Optional) Sequence identifier (default: SQ01)

#### Example
```bash
./scripts/process_and_push.sh ~/data/shots.csv ~/videos/footage.mp4 MY_PROJECT SQ01
```

## Output

All scripts create a `processed` directory in the current working directory to store output files.

## Permissions

Scripts automatically handle permissions by:
- Running Docker containers as root
- Setting 777 permissions on the output directory before and after execution