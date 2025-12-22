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
            data = json.load(f)                         #lendo conteudo json
            items = data.get("items",[])
            all_items.extend(items)

    return all_items

def transform_items(items: list) -> pd.DataFrame:
    rows = []

    for item in items:
        track = item.get("track", {})
        album = track.get("album", {})
        artists = track.get("artists", [])

        rows.append({
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
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("⚠️ Silver: DataFrame vazio — nenhuma linha gerada")
        return df

    if "played_at" in df.columns:
        df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce")

        '''coerce - se algum valor não puder ser convertido para datetime, 
        ele será transformado em NaT (Not a Time), evitando erros de execução.'''

    if "album_release_date" in df.columns:
        df["album_release_date"] = pd.to_datetime(
            df["album_release_date"], errors="coerce"
        ).dt.date

    return df


def save_silver_incremental(df_new: pd.DataFrame): #parametro passado: tudo da raw
    if df_new.empty:
        print("⚠️ Nada novo para salvar na Silver")
        return

    if SILVER_FILE.exists():
        '''Leitura da Silver atual, que contém todos os registros salvos anteriormente.
            parse_dates=["played_at"] garante que a coluna played_at será lida como datetime,
            para facilitar comparações.
        '''
        df_existing = pd.read_csv(SILVER_FILE, parse_dates = ["played_at"]) 
    
        # Mantém só o que ainda não existe 
        '''
            Cruze df_new com df_existing usando track_id e played_at,
            mantendo todas as linhas de df_new
            e me diga se cada linha encontrou correspondência ou não
        ''' 
        df_merged = df_new.merge(
            df_existing[["track_id","played_at"]],
            on = ["track_id","played_at"],
            how = "left",                   #“Tudo que está no df_new e ainda não está na Silver”
            indicator = True
        )

        df_to_insert = df_merged[df_merged["_merge"] == "left_only"] #“Quero somente os registros que ainda não existem na Silver”
        df_to_insert = df_to_insert.drop(columns="_merge") #Remove a coluna técnica _merge que só serve para controle interno

    else:
        df_to_insert = df_new

    if df_to_insert.empty:
        print("⚠️ Silver já está atualizada — nenhum novo registro")
        return

    #Salvamento do arquivo
    if SILVER_FILE.exists():
        df_to_insert.to_csv(SILVER_FILE, mode="a", index=False, header=False)
    else:
        df_to_insert.to_csv(SILVER_FILE, index=False)

    print(f"✅ Silver incrementada com {len(df_to_insert)} novos registros")

def run_silver():
    items = read_raw_files()
    print(f"🔎 Total de items lidos da Bronze: {len(items)}")
    df = transform_items(items)
    save_silver_incremental(df)
