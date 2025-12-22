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
def build_dim_artist_incremental(df: pd.DataFrame):
    # 1. Criação da dimensão a partir da silver
    dim_new = (
        df[["artist_id", "artist_name"]]
        .drop_duplicates(subset=["artist_id"])
        .reset_index(drop=True)
        .copy()
    )

    if dim_new.empty:
        print("⚠️ Nenhum artista novo para processar")
        return
    
    # 2. Se a tabela dimensão já existe, fazer carga incremental
    if DIM_ARTIST_FILE.exists():
        dim_existing = pd.read_csv(DIM_ARTIST_FILE)

        # Fazer o left join
        dim_merged = dim_new.merge(
            dim_existing[["artist_id"]],
            on="artist_id",
            how="left",
            indicator=True
        )

        dim_to_insert = (
            dim_merged[dim_merged["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
    else:
        # primeira carga
        dim_to_insert = dim_new

    if dim_to_insert.empty:
        print("⚠️ DIM Artist já está atualizada — nenhum novo registro")
        return
    
    # 3. Salvando incrementalmente
    if DIM_ARTIST_FILE.exists():
        dim_to_insert.to_csv(DIM_ARTIST_FILE, mode="a", index=False, header=False)
    else:
        dim_to_insert.to_csv(DIM_ARTIST_FILE, index=False)
    
    print(f"✅ DIM Artist incrementada com {len(dim_to_insert)} registros")


def build_dim_album_incremental(df: pd.DataFrame):
    # 1. Criação da dimensão a partir da silver
    dim_new = (
        df[["album_id", "album_name", "album_release_date", "artist_id"]]
        .drop_duplicates(subset=["album_id"])
        .reset_index(drop=True)
        .copy()
    )

    if dim_new.empty:
        print("⚠️ Nenhum álbum novo para processar")
        return
    
    # 2. Se a tabela dimensão já existe, fazer carga incremental
    if DIM_ALBUM_FILE.exists():
        dim_existing = pd.read_csv(DIM_ALBUM_FILE)

        # Fazer left join
        dim_merged = dim_new.merge(
            dim_existing[["album_id"]],
            on="album_id",
            how="left",
            indicator=True
        )

        dim_to_insert = (
            dim_merged[dim_merged["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
    else:
        # primeira carga
        dim_to_insert = dim_new
    
    if dim_to_insert.empty:
        print("⚠️ DIM Album já está atualizada — nenhum novo registro")
        return
    
    # 3. Salvando incrementalmente
    if DIM_ALBUM_FILE.exists():
        dim_to_insert.to_csv(DIM_ALBUM_FILE, mode="a", index=False, header=False)
    else:
        dim_to_insert.to_csv(DIM_ALBUM_FILE, index=False)

    print(f"✅ DIM Álbum incrementada com {len(dim_to_insert)} registros")


def build_dim_track_incremental(df: pd.DataFrame):
    # 1. Criação da dimensão a partir da silver
    dim_new = (
        df[["track_id", "track_name", "explicit", "popularity"]]
        .drop_duplicates(subset=["track_id"])
        .reset_index(drop=True)
        .copy()
    )

    if dim_new.empty:
        print("⚠️ Nenhuma faixa nova para processar")
        return

    # 2. Se a tabela dimensão já existe, fazer carga incremental
    if DIM_TRACK_FILE.exists():
        dim_existing = pd.read_csv(DIM_TRACK_FILE)

        # Fazer left join
        dim_merged = dim_new.merge(
            dim_existing[["track_id"]],
            on="track_id",
            how="left",
            indicator=True
        )

        dim_to_insert = (
            dim_merged[dim_merged["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
    else:
        # Primeira carga
        dim_to_insert = dim_new
    
    if dim_to_insert.empty:
        print("⚠️ DIM Faixa já está atualizada — nenhum novo registro")
        return
    
    # 3. Salvando incrementalmente
    if DIM_TRACK_FILE.exists():
        dim_to_insert.to_csv(DIM_TRACK_FILE, mode="a", index=False, header=False)
    else:
        dim_to_insert.to_csv(DIM_TRACK_FILE, index=False)


# =========================
# Fato
# =========================
def build_fact_recently_played_incremental(df: pd.DataFrame):
    # 1. Criação da tabela fato a partir da Silver
    fact_new = df[
        ["played_at", "track_id", "album_id", "duration_ms"]
    ].copy()

    if fact_new.empty:
        print("⚠️ Nenhum dado novo para a Fato")
        return

    # 2. Se a tabela fato já existe, fazer carga incremental
    if FACT_FILE.exists():
        fact_existing = pd.read_csv(FACT_FILE, parse_dates=["played_at"])

        # Left anti join
        fact_merged = fact_new.merge(
            fact_existing[["played_at", "track_id"]],
            on=["played_at", "track_id"],
            how="left",
            indicator=True
        )

        fact_to_insert = (
            fact_merged[fact_merged["_merge"] == "left_only"]
            .drop(columns="_merge")
        )
    else:
        # Primeira carga
        fact_to_insert = fact_new

    if fact_to_insert.empty:
        print("⚠️ Tabela Fato já está atualizada — nenhum novo registro")
        return

    # 3. Salvando incrementalmente
    if FACT_FILE.exists():
        fact_to_insert.to_csv(FACT_FILE, mode="a", index=False, header=False)
    else:
        fact_to_insert.to_csv(FACT_FILE, index=False)

    print(f"✅ Fato incrementada com {len(fact_to_insert)} novos registros")


# =========================
# Pipeline Gold
# =========================
def run_gold():
    print("🥇 Iniciando camada GOLD")

    df = load_silver()

    build_dim_artist_incremental(df)
    build_dim_album_incremental(df)
    build_dim_track_incremental(df)
    build_fact_recently_played_incremental(df)

    print("✅ Camada GOLD criada com sucesso")
