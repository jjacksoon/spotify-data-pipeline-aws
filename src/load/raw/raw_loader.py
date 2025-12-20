from pathlib import Path
from datetime import datetime
import json

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
# sobe: raw → load → src → etl_spotify

def save_recently_played_raw(data: dict) -> Path:
    base_path = BASE_DIR / "data" / "raw" / "spotify" / "recently_played"

    extraction_date = datetime.now().strftime("%Y-%m-%d")
    extraction_time = datetime.now().strftime("%Y%m%dT%H%M%S")

    folder = base_path / f"extraction_date={extraction_date}"
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"recently_played_{extraction_time}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return file_path
