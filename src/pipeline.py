import json
from pathlib import Path

# Importações dos módulos refatorados para AWS
from src.extract.spotify.user_recently_played import get_recently_played
from src.load.raw.raw_loader import save_recently_played_raw_to_s3 
from src.load.db.create_tables import create_tables
from src.transform.silver.silver_recently_played import run_silver
from src.transform.gold.gold_recently_played import run_gold # Importação da Camada Gold

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"

def load_access_token() -> str:
    """Carrega o token de acesso do Spotify salvo localmente."""
    with open(TOKEN_PATH, encoding="utf-8") as f:
        return json.load(f)["access_token"]

def run_pipeline():
    print("🚀 Iniciando Pipeline Spotify Cloud (End-to-End)...")

    # 1. Infraestrutura (RDS)
    # Garante que os Schemas (Raw, Silver, Gold) e tabelas iniciais existam no Postgres
    create_tables()
    print("🗄️ Estrutura de Schemas e Tabelas garantida no RDS")

    # 2. Extract + Load Raw (S3)
    # Busca dados novos na API do Spotify e salva o JSON bruto no S3
    token = load_access_token()
    data = get_recently_played(token, limit=10)
    
    s3_key_raw = save_recently_played_raw_to_s3(data)
    print(f"📥 Dados brutos (JSON) enviados para S3 Raw: {s3_key_raw}")

    # 3. Transform Silver (S3 + RDS)
    # Lê todos os JSONs da Raw, limpa, remove duplicatas e salva o CSV consolidado
    # Também sincroniza a tabela silver.recently_played no banco
    run_silver()
    print("🥈 Camada SILVER processada: S3 e RDS atualizados.")

    # 4. Transform Gold (S3 + RDS)
    # Pega o dado limpo da Silver e separa em Dimensões e Fatos (Star Schema)
    # Esta é a camada que o Power BI ou o DBeaver usam para análises
    run_gold()
    print("🥇 Camada GOLD processada: Dimensões e Fatos criadas.")

    print("\n--- STATUS FINAL DO PIPELINE ---")
    print("✅ INFRA  : RDS pronto")
    print("✅ RAW    : JSONs no S3")
    print("✅ SILVER : Tabela única limpa")
    print("✅ GOLD   : Star Schema pronto para BI")
    print("--------------------------------")

if __name__ == "__main__":
    run_pipeline()