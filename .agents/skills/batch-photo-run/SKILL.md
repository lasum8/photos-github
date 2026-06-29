---
name: batch-photo-run
description: Process newly added photos in this repository into optimized WebP assets and metadata manifest entries for the website. Use when the user adds a batch of originals and wants them tagged, optimized, and validated without committing or pushing.
---

# Batch Photo Run

Use this skill in `/Users/victorwu/photos-github` when new photos have been added under `originals/` and need to match the existing website photo pipeline.

## Ground Rules

- Do not commit or push unless the user explicitly asks.
- Do not add `Co-authored-by: Codex` to any commit message if a commit is later requested.
- Preserve user changes. Check `git status --short` before and after.
- Prefer the repo venv: `.venv/bin/python`.
- The expected pipeline files are:
  - `sync_metadata.py`
  - `process_photos.py`
  - `user_metadata.csv`
  - `optimized/manifest.json`
  - `originals/`
  - `optimized/`

## Workflow

1. Inspect the batch:

```bash
git status --short
rg --files
```

Identify untracked or newly modified files in `originals/`. New optimized files should normally be absent at first.

2. Read the repo scripts before running them:

```bash
sed -n '1,260p' process_photos.py
sed -n '1,220p' sync_metadata.py
sed -n '1,120p' user_metadata.csv
```

Current behavior:
- `sync_metadata.py` scans `originals/`, converts HEIC/HIF to AVIF when needed, and adds missing rows to `user_metadata.csv`.
- `process_photos.py` scans supported originals, hashes them, creates `optimized/<stem>.webp` at max dimension 2400 and quality 85, extracts EXIF dates, merges CSV metadata, and writes `optimized/manifest.json`.

3. Ensure dependencies:

```bash
.venv/bin/python -c 'import PIL, pillow_heif, pillow_avif, pandas, tqdm; print("venv deps ok")'
```

If this fails, diagnose the venv before installing anything. Ask for approval before network installs.

4. Create a contact sheet for visual tagging when the user gives subjective categories:

```bash
.venv/bin/python - <<'PY'
from PIL import Image, ImageOps, ImageDraw
from pathlib import Path

files = sorted(Path('originals').glob('_SDI04*.JPG'))
thumbs = []
for p in files:
    with Image.open(p) as im:
        im = ImageOps.exif_transpose(im).convert('RGB')
        im.thumbnail((360, 240))
        canvas = Image.new('RGB', (380, 285), 'white')
        canvas.paste(im, ((380 - im.width) // 2, 10))
        ImageDraw.Draw(canvas).text((10, 255), p.name, fill=(0, 0, 0))
        thumbs.append(canvas)

cols = 2
rows = (len(thumbs) + cols - 1) // cols
sheet = Image.new('RGB', (cols * 380, rows * 285), (240, 240, 240))
for i, thumb in enumerate(thumbs):
    sheet.paste(thumb, ((i % cols) * 380, (i // cols) * 285))

out = Path('/private/tmp/new_photo_contact_sheet.jpg')
sheet.save(out, quality=90)
print(out)
PY
```

Adjust the glob to the actual batch. Open the contact sheet visually before deciding tags.

5. Sync metadata rows:

```bash
.venv/bin/python sync_metadata.py
```

6. Edit `user_metadata.csv` for the new rows.

Conventions observed in this repo:
- `location` is a simple place name, for example `Toronto`, `Kyoto`, `New York`.
- `tags` are semicolon-separated.
- Put color mode first: `color` or `B&W`.
- Existing subject tags include `flower`, `food`, and `people`.
- It is acceptable to add a clear new subject tag when the user requests it, for example `nature` or `art gallery`.

7. Process photos:

```bash
.venv/bin/python process_photos.py
```

If this fails with `PermissionError` from `concurrent.futures.process` / `SC_SEM_NSEMS_MAX`, rerun the same command with sandbox escalation. This repo's processor uses `ProcessPoolExecutor`, which may need OS semaphore access.

8. Validate outputs:

```bash
git status --short
ls -l optimized/<new-stem>.webp
.venv/bin/python - <<'PY'
import json

names = [
    # Fill with the new original filenames, e.g. "_SDI0410.JPG"
]

with open('optimized/manifest.json') as f:
    data = json.load(f)

for name in names:
    entry = data[name]
    print(name, entry.get('optimized_path'), entry.get('location'), ';'.join(entry.get('tags', [])), entry.get('date_taken', 'NO_DATE'))
PY
```

Also inspect dimensions if useful:

```bash
.venv/bin/python - <<'PY'
from PIL import Image
from pathlib import Path

for p in sorted(Path('optimized').glob('<batch-glob>.webp')):
    with Image.open(p) as im:
        print(p.name, im.size)
PY
```

Expected dimensions have neither side above 2400.

## Final Report

Summarize:
- New originals found.
- Metadata tags assigned.
- WebP outputs generated.
- Manifest validation result.
- Any commands that required sandbox escalation.
- Current git status at a high level.
