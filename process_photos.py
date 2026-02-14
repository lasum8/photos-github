import os
import json
import hashlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from PIL import Image, ExifTags
import pillow_heif
from tqdm import tqdm
from datetime import datetime

# Configuration
SOURCE_DIR = Path("originals")
DEST_DIR = Path("optimized")
MANIFEST_FILE = DEST_DIR / "manifest.json"
USER_METADATA_FILE = Path("user_metadata.json")
MAX_DIMENSION = 2400  # Max width or height
QUALITY = 85
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.heif'}

# Register HEIF opener
pillow_heif.register_heif_opener()

def get_exif_data(img):
    """Extract basic EXIF data like date taken."""
    exif_data = {}
    try:
        exif = img.getexif()
        if not exif:
            return exif_data

        for key, val in exif.items():
            if key in ExifTags.TAGS:
                decoded_key = ExifTags.TAGS[key]
                if decoded_key == "DateTimeOriginal":
                    exif_data['date_taken'] = val
                elif decoded_key == "DateTime":
                     if 'date_taken' not in exif_data: # Fallback
                        exif_data['date_taken'] = val

        # Standardize date format if found (YYYY:MM:DD HH:MM:SS -> ISO 8601)
        if 'date_taken' in exif_data:
            try:
                dt_obj = datetime.strptime(exif_data['date_taken'], '%Y:%m:%d %H:%M:%S')
                exif_data['date_taken'] = dt_obj.isoformat()
            except ValueError:
                pass # Keep original string if format is weird

    except Exception as e:
        pass # EXIF data might be missing or corrupt
    return exif_data

def calculate_file_hash(filepath):
    """Calculate MD5 hash of a file efficiently."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        # Read in chunks to avoid memory issues with large files
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def process_image(task):
    """
    Process a single image: resize and save as WebP.
    Returns (filename, hash, success_bool, metadata_dict)
    """
    filename, file_hash = task
    source_path = SOURCE_DIR / filename
    dest_path = DEST_DIR / f"{source_path.stem}.webp"

    try:
        # Open and extract Metadata before any modifications
        metadata = {}
        with Image.open(source_path) as img:
            metadata = get_exif_data(img)
            # Re-open or copy for processing to avoid closed file issues
            # Actually, we can just continue using 'img' context

            # Handle orientation from EXIF
            from PIL import ImageOps
            img = ImageOps.exif_transpose(img)

            # Calculate new dimensions
            width, height = img.size
            if width > MAX_DIMENSION or height > MAX_DIMENSION:
                ratio = min(MAX_DIMENSION / width, MAX_DIMENSION / height)
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)

            # Save as WebP
            img.save(dest_path, "WEBP", quality=QUALITY)

        return filename, file_hash, True, metadata
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return filename, file_hash, False, {}

def load_json(path):
    if path.exists():
        with open(path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_json(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def main():
    # Ensure destination exists
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    current_manifest = load_json(MANIFEST_FILE)
    user_metadata = load_json(USER_METADATA_FILE)

    new_manifest = {}
    tasks = []

    print(f"Scanning '{SOURCE_DIR}' for new or modified photos...")

    # scan files
    files = [
        f for f in os.listdir(SOURCE_DIR)
        if Path(f).suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    # Pre-calculate hashes and determine work
    for filename in tqdm(files, desc="Checking files"):
        filepath = SOURCE_DIR / filename
        file_hash = calculate_file_hash(filepath)

        dest_file = DEST_DIR / f"{Path(filename).stem}.webp"
        needs_processing = False

        # Check current entry
        entry = current_manifest.get(filename)

        # Trigger re-process if:
        # 1. New file
        # 2. Old manifest format (str instead of dict)
        # 3. Hash mismatch
        # 4. Output dest missing
        if not entry:
            needs_processing = True
        elif isinstance(entry, str):
            needs_processing = True
        elif isinstance(entry, dict) and entry.get('hash') != file_hash:
            needs_processing = True
        elif not dest_file.exists():
            needs_processing = True

        if needs_processing:
            tasks.append((filename, file_hash))
        else:
            # COPY existing entry to new manifest
            new_manifest[filename] = entry

            # MERGE user metadata overrides immediately
            if filename in user_metadata:
                new_manifest[filename].update(user_metadata[filename])

    print(f"Processing {len(tasks)} photos...")

    if tasks:
        with ProcessPoolExecutor() as executor:
            results = list(tqdm(executor.map(process_image, tasks), total=len(tasks), desc="Optimizing"))

            for filename, file_hash, success, extracted_meta in results:
                if success:
                    # Create baseline entry
                    entry = {
                        "hash": file_hash,
                        "filename": filename,
                        "optimized_path": f"optimized/{Path(filename).stem}.webp",
                        **extracted_meta
                    }

                    # Merge user manual metadata
                    if filename in user_metadata:
                        entry.update(user_metadata[filename])

                    new_manifest[filename] = entry

    # Save the updated manifest
    save_json(new_manifest, MANIFEST_FILE)
    print("Done! Photos optimized and manifest updated.")

if __name__ == "__main__":
    main()
