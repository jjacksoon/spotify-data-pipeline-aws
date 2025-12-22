from pathlib import Path
import pandas as pd

# Diretórios
BASE_DIR = Path(__file__).resolve().parents[3]
SILVER_FILE = BASE_DIR / "data" / "silver" / "recently_played.csv"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# Arquivos Gold
FACT_FILE = GOLD_DIR / "fact_recently_played.csv"
DIM_ARTIST_FILE = GOLD_DIR / "dim_artist.csv"
DIM_ALBUM_FILE = GOLD_DIR / "dim_album.csv"
DIM_TRACK_FILE = GOLD_DIR / "dim_track.csv"


# =========================
# Leitura da Silver
# =========================
def load_silver() -> pd.DataFrame:
    return pd.read_csv(SILVER_FILE, parse_dates=["played_at"])


# =========================
# Dimensões
# =========================
def build_dim_artist(df: pd.DataFrame):
    dim_artist = (
        df[["artist_id", "artist_name"]]
        .drop_duplicates(subset=["artist_id"])
        .reset_index(drop=True)
    )
    dim_artist.to_csv(DIM_ARTIST_FILE, index=False)


def build_dim_album(df: pd.DataFrame):
    dim_album = (
        df[["album_id", "album_name", "album_release_date", "artist_id"]]
        .drop_duplicates(subset=["album_id"])
        .reset_index(drop=True)
    )
    dim_album.to_csv(DIM_ALBUM_FILE, index=False)


def build_dim_track(df: pd.DataFrame):
    dim_track = (
        df[["track_id", "track_name", "explicit", "popularity"]]
        .drop_duplicates(subset=["track_id"])
        .reset_index(drop=True)
    )
    dim_track.to_csv(DIM_TRACK_FILE, index=False)


# =========================
# Fato
# =========================
def build_fact_recently_played(df: pd.DataFrame):
    fact = df[
        ["played_at", "track_id", "album_id", "duration_ms"]
    ].copy()

    fact.to_csv(FACT_FILE, index=False)


# =========================
# Pipeline Gold
# =========================
def run_gold():
    print("🥇 Iniciando camada GOLD")

    df = load_silver()

    build_dim_artist(df)
    build_dim_album(df)
    build_dim_track(df)
    build_fact_recently_played(df)

    print("✅ Camada GOLD criada com sucesso")
