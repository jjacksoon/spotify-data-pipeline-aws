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
    """Função genérica para Carga Incremental na Gold (S3 + RDS).
    pk_columns: colunas que identificam se o registro é único (ex: artist_id)
    """
    s3_key = f"gold/{table_name}.csv"

    # --- 1. LÓGICA INCREMENTAL NO S3 ---
    try:
        #tente ler o que já existe na gold do S3
        response = s3_client.get_object(Bucket = BUCKET_NAME, Key = s3_key)
        df_existing = pd.read_csv(response['Body'])

        #Faça o merge para identificar o que é novo (Left Anti-Join)
        df_merged = df_new.merge(
            df_existing[pk_columns], on = pk_columns, how = "left", indicator= True
        )
        df_to_insert = df_merged[df_merged["_merge"]== "left_only"].drop(columns="_merge")

        #DataFrame final consolidado
        df_final = pd.concat([df_existing, df_to_insert], ignore_index = True)

    except s3_client.exceptions.NoSuchKey:
        #Se a tabela não existe, tudo é novo
        print(f"✨ Criando nova tabela Gold no S3: {table_name}")
        df_final = df_new           #Se não existe tabela  → o resultado final é exatamente o dado novo
        df_to_insert = df_final     #O que será persistido no S3, ou seja, tudo que chegou agora será inserido
    
    if df_to_insert.empty:
        print(f"⚠️ {table_name}: Sem registros novos.")
    else:
        #Salva o arquivo completo de volta no S3
        csv_buffer = StringIO()
        df_final.to_csv(csv_buffer, index = False)
        s3_client.put_object(
            Bucket = BUCKET_NAME,
            Key = s3_key, 
            Body = csv_buffer.getvalue()
        )
        print(f"✅ {table_name} atualizada no S3: +{len(df_to_insert)} linhas.")

    
    # --- 2. SINCRONIZAÇÃO COM O RDS ---
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # O segredo é envolver a string no text()
        conn.execute(text(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE;"))
        # No SQLAlchemy 2.0, o commit deve ser explícito em conexões manuais
        conn.commit()

    # Agora o Pandas segue com o processo normal
    df_final.to_sql(
        table_name, 
        con=engine, 
        schema='gold', 
        if_exists='replace', 
        index=False
    )
    print(f"🏆 RDS: gold.{table_name} sincronizada ({len(df_final)} total).")


def run_gold():
    print(f"🥇 Iniciando processamento GOLD (Cloud)...")
    
    # Carregando dados da Silver (S3)
    df = load_silver_from_s3()

    # --- PROCESSAMENTO DAS TABELAS DIMENSÕES ---

    # Artist (unique by artist_id)
    dim_artist = df[["artist_id", "artist_name"]].drop_duplicates(subset = ["artist_id"])
    save_gold_incremental(dim_artist, "dim_artist", ["artist_id"])

    # Album (unique by album_id)
    dim_album = df[["album_id", "album_name", "album_release_date", "artist_id"]].drop_duplicates(subset = "album_id")
    save_gold_incremental(dim_album, "dim_album", ["album_id"])

    # Track (unique by track_id)
    dim_track = df[["track_id", "track_name", "explicit", "popularity"]].drop_duplicates(subset = "track_id")
    save_gold_incremental(dim_track, "dim_track", ["track_id"])

    # --- PROCESSAMENTO DA TABELA FATO

    fact_recently_played =  df[["played_at", "track_id", "album_id", "duration_ms"]]
    save_gold_incremental(fact_recently_played, "fact_recently_played", ["played_at", "track_id"])

    print("🏁 Camada GOLD finalizada com sucesso!")

