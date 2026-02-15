import os
import pandas as pd
from pathlib import Path
from PIL import Image
import pillow_heif
import pillow_avif

# Register plugins
pillow_heif.register_heif_opener()

# Configuration
ORIGINALS_DIR = Path("originals")
METADATA_FILE = Path("user_metadata.csv")
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.avif', '.heic', '.hif'}

def convert_to_avif(filepath):
    """Converts a HEIC/HIF file to AVIF in place and returns new path."""
    new_path = filepath.with_suffix(".AVIF")

    print(f"Auto-converting {filepath.name} to AVIF...")
    try:
        with Image.open(filepath) as img:
            img.save(new_path, 'AVIF', quality=95)

        # Remove original HEIC
        os.remove(filepath)
        return new_path

    except Exception as e:
        print(f"Error converting {filepath.name}: {e}")
        return filepath

def main():
    if not ORIGINALS_DIR.exists():
        print(f"Directory '{ORIGINALS_DIR}' does not exist.")
        return

    # Load existing CSV or create new DataFrame
    if METADATA_FILE.exists():
        try:
            df = pd.read_csv(METADATA_FILE).fillna("")
            # Ensure columns exist even if file was empty
            if 'filename' not in df.columns:
                df = pd.DataFrame(columns=['filename', 'location', 'tags'])
        except Exception:
             df = pd.DataFrame(columns=['filename', 'location', 'tags'])
    else:
        df = pd.DataFrame(columns=['filename', 'location', 'tags'])

    # 1. Scan for files
    # We iterate a list() copy because we might modify the directory (renaming heic->avif)
    raw_files = [f for f in ORIGINALS_DIR.iterdir() if f.suffix.lower() in SUPPORTED_EXTENSIONS]

    final_filenames = []

    for filepath in raw_files:
        # Handle conversion immediately so metadata key matches final filename
        if filepath.suffix.lower() in ['.heic', '.hif']:
            new_path = convert_to_avif(filepath)
            final_filenames.append(new_path.name)
        else:
            final_filenames.append(filepath.name)

    # 2. Sync with Metadata
    # Convert existing filenames to set for O(1) lookup
    existing_filenames = set(df['filename'].astype(str))
    new_entries = []

    for filename in final_filenames:
        if filename not in existing_filenames:
            new_entries.append({
                "filename": filename,
                "location": "",
                "tags": ""
            })
            print(f"+ Added new entry: {filename}")

    if new_entries:
        new_df = pd.DataFrame(new_entries)
        # Fix for future warning: use concat instead of appending to frame is handled by concat a list of dicts via dataframe
        df = pd.concat([df, new_df], ignore_index=True)

        # Sort by filename
        df = df.sort_values('filename')

        df.to_csv(METADATA_FILE, index=False)
        print(f"\nSuccessfully added {len(new_entries)} new entries to {METADATA_FILE}.")
    else:
        print("\nNo new photos found to add.")
        # Re-sort and save anyway to ensure consistency
        df = df.sort_values('filename')
        df.to_csv(METADATA_FILE, index=False)

if __name__ == "__main__":
    main()
