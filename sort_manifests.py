import json
from pathlib import Path

FILES_TO_SORT = ["user_metadata.json", "optimized/manifest.json"]

def sort_json_file(filepath):
    path = Path(filepath)
    if not path.exists():
        print(f"Skipping {path}: not found")
        return

    print(f"Sorting {path}...")
    with open(path, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"Error decoding {path}")
            return

    # Sort dictionary by keys (filenames)
    # We use a regular sort which is alphanumeric (ASCIIbetical)
    # _SDI... comes after DSC... usually? No, '_' is ASCII 95, 'D' is 68.
    # Actually 'D' comes before '_'.
    # 'DSC' vs '_SDI'.
    # 0-9 < A-Z < _ < a-z
    # So DSC... will likely come before _SDI...
    # That is alphanumeric sorting.
    sorted_data = dict(sorted(data.items()))

    with open(path, 'w') as f:
        json.dump(sorted_data, f, indent=2)
    print(f"Sorted {len(sorted_data)} entries in {path}")

if __name__ == "__main__":
    for f in FILES_TO_SORT:
        sort_json_file(f)
