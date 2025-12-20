import json
from pathlib import Path
from datetime import datetime
import pandas as pd 


#Diretórios
BASE_DIR = Path(__file__).resolve().parents[3]
BRONZE_DIR = BASE_DIR/"data"/"raw" 
SILVER_DIR = BASE_DIR/"data"/"silver"
SILVER_DIR.mkdir(parents = True, exist_ok = True)

SILVER_FILE = SILVER_DIR/"recently_played.csv"

#Leitura da camada bronze
def read_raw_files():
    raw_files = list(BRONZE_DIR.rglob("recently_played_*.json")) #leitura de todos arquivos da camada
    print(f"📂 Arquivos RAW encontrados: {len(raw_files)}")

    all_items = [] #juntando todos arquivos em uma unica lista
    for file in raw_files:
        print(f"📄 Lendo RAW: {file}")
        with open(file,"r", encoding="utf-8") as f:
            data = json.load(f)
            items = data.get("items",[])
            all_items.extend(items)

    return all_items

def transform_items(items: list) -> pd.DataFrame:
    rows = []

    for item in items:
        track = item.get("track", {})
        album = track.get("album", {})
        artists = track.get("artists", [])

        row = {
            "played_at": item.get("played_at"),
            "track_id": track.get("id"),
            "track_name": track.get("name"),
            "duration_ms": track.get("duration_ms"),
            "popularity": track.get("popularity"),
            "explicit": track.get("explicit"),
            "album_id": album.get("id"),
            "album_name": album.get("name"),
            "album_release_date": album.get("release_date"),
            "artist_id": artists[0].get("id") if artists else None,
            "artist_name": artists[0].get("name") if artists else None,
            "load_date": datetime.now().date()
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        print("⚠️ Silver: DataFrame vazio — nenhuma linha gerada")
        return df

    if "played_at" in df.columns:
        df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce")

    if "album_release_date" in df.columns:
        df["album_release_date"] = pd.to_datetime(
            df["album_release_date"], errors="coerce"
        ).dt.date

    return df


def save_silver(df : pd.DataFrame):
    if SILVER_FILE.exists():
        df.to_csv(SILVER_FILE, mode = "a", index = False, header = False)
    else:
        df.to_csv(SILVER_FILE, index = False)


'''def run_silver():
    items = read_raw_files()
    df = transform_items(items)
    save_silver(df)
'''

def run_silver():
    items = read_raw_files()
    print(f"🔎 Total de items lidos da Bronze: {len(items)}")
    df = transform_items(items)
    save_silver(df)

if __name__ == "__main__":
    run_silver()
