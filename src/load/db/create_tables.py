import psycopg2
from dotenv import load_dotenv
import os

# 1. Carregar variáveis do .env
load_dotenv()

def create_tables():
    """
    Conecta ao RDS e cria a estrutura de schemas e tabelas para o projeto Spotify.
    """
    conn = None
    try:
        # 2. Criar conexão usando as variáveis do seu .env
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        
        cursor = conn.cursor()

        # --- SCHEMA SILVER ---
        print("🛠️ Criando Schema SILVER...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS silver.recently_played(
                played_at TIMESTAMP,
                track_id VARCHAR,
                track_name VARCHAR,
                duration_ms INT,
                popularity INT,
                explicit BOOLEAN,
                album_id VARCHAR,
                album_name VARCHAR,
                album_release_date DATE,
                artist_id VARCHAR,
                artist_name VARCHAR,
                load_date DATE,
                CONSTRAINT pk_recently_played PRIMARY KEY (played_at, track_id)
            );
        """)

        # --- SCHEMA GOLD ---
        print("🛠️ Criando Schema GOLD e Dimensões...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        # Dimensão Artista
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.dim_artist(
                artist_id VARCHAR PRIMARY KEY,
                artist_name VARCHAR
            );
        """)

        # Dimensão Álbum
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.dim_album(
                album_id VARCHAR PRIMARY KEY,
                album_name VARCHAR,
                album_release_date DATE,
                artist_id VARCHAR REFERENCES gold.dim_artist(artist_id)
            );
        """)

        # Dimensão Música (Track)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.dim_track(
                track_id VARCHAR PRIMARY KEY,
                track_name VARCHAR,
                explicit BOOLEAN,
                popularity INT
            );
        """)

        # --- TABELA FATO ---
        print("🛠️ Criando Tabela Fato...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.fact_recently_played(
                played_at TIMESTAMP,
                track_id VARCHAR REFERENCES gold.dim_track(track_id),
                album_id VARCHAR REFERENCES gold.dim_album(album_id),
                duration_ms INT,
                PRIMARY KEY (played_at, track_id)
            );
        """)

        # 3. Confirmar alterações
        conn.commit()
        print("✅ Estrutura de banco de dados criada com sucesso no RDS!")

    except Exception as e:
        print(f"❌ Erro ao conectar ou criar tabelas no RDS: {e}")
        if conn:
            conn.rollback()
    
    finally:
        # 4. Fechar cursor e conexão de forma segura
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()