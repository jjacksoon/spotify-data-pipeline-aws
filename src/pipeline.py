import json
from pathlib import Path

# Importações dos módulos refatorados para AWS
from src.extract.spotify.user_recently_played import get_recently_played
from src.load.raw.raw_loader import save_recently_played_raw_to_s3 
from src.load.db.create_tables import create_tables
from src.transform.silver.silver_recently_played import run_silver # Nova importação

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"

def load_access_token() -> str:
    with open(TOKEN_PATH, encoding="utf-8") as f:
        return json.load(f)["access_token"]

def run_pipeline():
    print("🚀 Iniciando Pipeline Spotify Cloud...")

    # 1. Infraestrutura: Garantir Schemas e Tabelas no RDS
    # O DROP SCHEMA public CASCADE e as criações rodam aqui
    create_tables()
    print("🗄️ Estrutura de Schemas e Tabelas garantida no RDS")

    # 2. Extract + Load Raw: API Spotify -> S3 (JSON)
    token = load_access_token()
    data = get_recently_played(token, limit=10)
    
    s3_key_raw = save_recently_played_raw_to_s3(data)
    print(f"📥 Dados brutos (JSON) enviados para S3 Raw: {s3_key_raw}")

    # 3. Transform Silver: S3 Raw (JSON) -> S3 Silver (CSV Incremental)
    # Aqui o Python lê do S3, limpa com Pandas e devolve para o S3
    run_silver()
    print("🥈 Camada SILVER processada e salva no S3 (Incremental)")

    print("\n--- STATUS DO PIPELINE ---")
    print("✅ INFRA  : RDS configurado")
    print("✅ RAW    : S3 Raw atualizado")
    print("✅ SILVER : S3 Silver atualizado")
    print("⏳ GOLD   : Pendente (Próximo passo)")
    print("⏳ DB LOAD: Pendente (Carga Silver/Gold no RDS)")
    print("--------------------------")

if __name__ == "__main__":
    run_pipeline()