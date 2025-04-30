# Kitsu Ingest Tool

A CLI utility to ingest breakdown CSVs and video previews into a Kitsu project.

---

## 🛠️ Requirements

- Python 3.9+
- FFmpeg installed and available in your system PATH
- A `.env` file in the project root containing:

```
KITSU_SERVER=https://your-kitsu-server/api
KITSU_EMAIL=your@email.com
KITSU_PASSWORD=yourpassword
```

---

## 📆 Dependencies

Install all dependencies with:

```
pip install -e .
```

Or manually:

```
pip install pandas ffmpeg-python gazu python-dotenv
```

---

## 🚀 Usage

### Basic CSV processing

```
kitsu-ingest --csv path/to/breakdown.csv
```

### Process video + push to Kitsu

```
kitsu-ingest --csv path/to/breakdown.csv \
             --video path/to/video.mp4 \
             --push KITSU_PROJECT_NAME
```

### Add a new version flag

```
kitsu-ingest --csv breakdown.csv \
             --video video.mp4 \
             --push KITSU_PROJECT \
             --new_version
```

### Specify sequence (optional)

```
kitsu-ingest --csv breakdown.csv --sequence SQ10
```

### Run as a module instead of entry point

```
python -m kitsu_ingest --csv breakdown.csv ...
```

