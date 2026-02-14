import os
import json
from pathlib import Path
from PIL import Image
import pillow_heif
import pillow_avif
from tqdm import tqdm

# Register HEIF/AVIF plugins
pillow_heif.register_heif_opener()

ORIGINALS_DIR = Path("originals")
METADATA_FILE = Path("user_metadata.json")

def load_json(path):
    if path.exists():
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_json(data, path):
    # Sort keys for consistent ordering
    sorted_data = dict(sorted(data.items()))
    with open(path, 'w') as f:
        json.dump(sorted_data, f, indent=2)

def main():
    metadata = load_json(METADATA_FILE)

    # Target all .HEIC files (case insensitive)
    heic_files = [
        f for f in ORIGINALS_DIR.glob("*")
        if f.suffix.lower() == '.heic'
    ]

    # Also handle .HIF if any are left over
    heic_files.extend([
        f for f in ORIGINALS_DIR.glob("*")
        if f.suffix.lower() == '.hif'
    ])

    if not heic_files:
        print("No HEIC/HIF files found to migrate.")
        return

    print(f"Found {len(heic_files)} files to migrate to AVIF.")

    converted_count = 0
    updated_meta_count = 0

    for heic_path in tqdm(heic_files, desc="Converting to AVIF"):
        avif_path = heic_path.with_suffix(".AVIF")

        # Check if already processed (AVIF exists and HEIC exists)
        if avif_path.exists() and avif_path.stat().st_size > 0:
            # Maybe previously interrupted?
            # We can trust the AVIF if file size is reasonable,
            # but to ensure "beauty" maybe safer to re-convert if needed?
            # Let's assume if it exists it's done, unless size is 0.
            pass
        else:
            try:
                # Open and Convert
                with Image.open(heic_path) as img:
                    # Save as AVIF with high quality (lossless-ish)
                    # quality=95 is usually considered transparent
                    img.save(avif_path, "AVIF", quality=95)
            except Exception as e:
                print(f"Error converting {heic_path}: {e}")
                continue

        # If AVIF success, handle metadata
        if avif_path.exists() and avif_path.stat().st_size > 0:
            filename_heic = heic_path.name
            filename_avif = avif_path.name

            # Update key in metadata
            if filename_heic in metadata:
                metadata[filename_avif] = metadata.pop(filename_heic)
                updated_meta_count += 1

            # Remove old HEIC file
            try:
                heic_path.unlink()
                converted_count += 1
            except OSError as e:
                print(f"Warning: could not delete {heic_path}: {e}")

    # Save updated metadata
    save_json(metadata, METADATA_FILE)

    print(f"\nMigration Complete.")
    print(f"Converted Files: {converted_count}")
    print(f"Metadata Updates: {updated_meta_count}")

if __name__ == "__main__":
    main()
