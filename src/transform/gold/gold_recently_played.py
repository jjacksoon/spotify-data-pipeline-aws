import os
import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

#Carregando variáveis de ambiente
load_dotenv()

#Configuração AWS e Banco
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
s3_client = boto3.client('s3')

def get_db_engine():
    """Cria conexão com o RDS PostgreSQL"""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database =os.getenv("DB_NAME")
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# =========================
# Leitura da Silver
# =========================
def load_silver_from_s3() -> pd.DataFrame:
    """lê a tabela silver consolidando direto no s3"""
    silver_key = "silver/recently_played.csv"
    response = s3_client.get_object(Bucket = BUCKET_NAME, Key = silver_key)
    
    #Lendo csv da memódia (Body) para o pandas
    return pd.read_csv(response['Body'], parse_dates=["played_at"])

def save_gold_incremental(df_new: pd.DataFrame, table_name: str, pk_columns: list):
    """
    Função Genérica para Carga Incremental na Gold (S3 + RDS) com tratamento de datas.
    """
    s3_key = f"gold/{table_name}.csv"
    
    # Garantir que se houver 'played_at' no novo dado, ele seja datetime com UTC
    if "played_at" in df_new.columns:
        df_new["played_at"] = pd.to_datetime(df_new["played_at"], utc=True)
    
    try:
        # Tenta ler o que já existe na Gold do S3
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        df_existing = pd.read_csv(response['Body'])
        
        # Garantir que o dado que veio do S3 também seja convertido para datetime UTC antes do merge
        if "played_at" in df_existing.columns:
            df_existing["played_at"] = pd.to_datetime(df_existing["played_at"], utc=True)
        
        # Agora o merge vai funcionar porque ambos são datetime64[ns, UTC]
        df_merged = df_new.merge(
            df_existing[pk_columns], on=pk_columns, how="left", indicator=True
        )
        df_to_insert = df_merged[df_merged["_merge"] == "left_only"].drop(columns="_merge")
        
        # DataFrame final consolidado
        df_final = pd.concat([df_existing, df_to_insert], ignore_index=True)
        
    except s3_client.exceptions.NoSuchKey:
        print(f"✨ Criando nova tabela Gold no S3: {table_name}")
        df_final = df_new
        df_to_insert = df_new

    # ... restante do código (salvamento no S3 e RDS) permanece igual ...
    if df_to_insert.empty:
        print(f"⚠️ {table_name}: Sem registros novos.")
    else:
        csv_buffer = StringIO()
        df_final.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"✅ {table_name} atualizada no S3: +{len(df_to_insert)} linhas.")

    engine = get_db_engine()
    
    # Se você adicionou o DROP CASCADE com text(), mantenha-o aqui:
    with engine.connect() as conn:
        from sqlalchemy import text
        conn.execute(text(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE;"))
        conn.commit()

    df_final.to_sql(table_name, con=engine, schema='gold', if_exists='replace', index=False)
    print(f"🏆 RDS: gold.{table_name} sincronizada ({len(df_final)} total).")

def run_gold():
    print("🥇 Iniciando processamento GOLD (Cloud)...")
    
    # Carrega os dados da Silver (S3)
    df = load_silver_from_s3()

    # --- PROCESSAMENTO DAS DIMENSÕES (Adicionando .copy()) ---
    
    # Artistas - Usamos .copy() no final para evitar o SettingWithCopyWarning
    dim_artist = df[["artist_id", "artist_name"]].drop_duplicates(subset=["artist_id"]).copy()
    save_gold_incremental(dim_artist, "dim_artist", ["artist_id"])

    # Álbuns - Adicionando .copy()
    dim_album = df[["album_id", "album_name", "album_release_date", "artist_id"]].drop_duplicates(subset=["album_id"]).copy()
    save_gold_incremental(dim_album, "dim_album", ["album_id"])

    # Faixas - Adicionando .copy()
    dim_track = df[["track_id", "track_name", "explicit", "popularity"]].drop_duplicates(subset=["track_id"]).copy()
    save_gold_incremental(dim_track, "dim_track", ["track_id"])

    # --- PROCESSAMENTO DA FATO ---
    
    # Fato - Adicionando .copy()
    fact_recently_played = df[["played_at", "track_id", "album_id", "duration_ms"]].copy()
    save_gold_incremental(fact_recently_played, "fact_recently_played", ["played_at", "track_id"])

    print("🏁 Camada GOLD finalizada com sucesso!")

