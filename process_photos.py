import os
import json
import hashlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from PIL import Image
import pillow_heif
from tqdm import tqdm

# Configuration
SOURCE_DIR = Path("originals")
DEST_DIR = Path("optimized")
MANIFEST_FILE = DEST_DIR / "manifest.json"
MAX_DIMENSION = 2400  # Max width or height
QUALITY = 85
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.heif'}

# Register HEIF opener
pillow_heif.register_heif_opener()

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
    Returns (filename, hash, success_bool)
    """
    filename, file_hash = task
    source_path = SOURCE_DIR / filename
    dest_path = DEST_DIR / f"{source_path.stem}.webp"

    try:
        with Image.open(source_path) as img:
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

        return filename, file_hash, True
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return filename, file_hash, False

def load_manifest():
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_manifest(manifest):
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(manifest, f, indent=2)

def main():
    # Ensure destination exists
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    current_manifest = load_manifest()
    new_manifest = {}
    tasks = []

    print(f"Scanning '{SOURCE_DIR}' for new or modified photos...")

    # scan files
    files = [
        f for f in os.listdir(SOURCE_DIR)
        if Path(f).suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    # Pre-calculate hashes and determine work
    # We do this in the main thread or could also parallelize if IO bound,
    # but hashing is fast enough for usually reasonable amounts of photos.
    for filename in tqdm(files, desc="Checking files"):
        filepath = SOURCE_DIR / filename
        file_hash = calculate_file_hash(filepath)

        # Check if needs processing
        # Re-process if:
        # 1. Not in manifest
        # 2. Hash changed
        # 3. Output file doesn't exist (deleted manually?)
        dest_file = DEST_DIR / f"{Path(filename).stem}.webp"

        if (filename not in current_manifest or
            current_manifest[filename] != file_hash or
            not dest_file.exists()):
            tasks.append((filename, file_hash))
        else:
            # Keep existing record
            new_manifest[filename] = file_hash

    if not tasks:
        print("No new photos to process.")
        # We might want to save manifest to prune deleted files
        save_manifest(new_manifest)
        return

    print(f"Processing {len(tasks)} photos...")

    # Run processing in parallel
    # Use generic 'dict' update for the manifest as results come in
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_image, tasks), total=len(tasks), desc="Optimizing"))

        for filename, file_hash, success in results:
            if success:
                new_manifest[filename] = file_hash

    # Save the updated manifest
    save_manifest(new_manifest)
    print("Done! Photos optimized.")

if __name__ == "__main__":
    main()
