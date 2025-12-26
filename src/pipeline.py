import json
from pathlib import Path
from src.extract.spotify.user_recently_played import get_recently_played
from src.load.raw.raw_loader import save_recently_played_raw_to_s3 
from src.load.db.create_tables import create_tables

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"

def load_access_token() -> str:
    with open(TOKEN_PATH, encoding="utf-8") as f:
        return json.load(f)["access_token"]

def run_pipeline():
    print("🚀 Iniciando Migração para Cloud...")

    # 1. Banco de Dados (Amazon RDS)
    # Testa se o Python consegue conectar e criar os Schemas Silver e Gold no RDS
    create_tables()
    print("🗄️ Estrutura de Schemas garantida no RDS")

    # 2. Extração + Carga Raw (Amazon S3)
    token = load_access_token()
    data = get_recently_played(token, limit=10)
    
    # Agora o dado não toca mais o seu disco rígido, vai direto pra nuvem
    s3_key = save_recently_played_raw_to_s3(data)
    print(f"📥 Dados brutos enviados para o S3: {s3_key}")

    print("\n--- STATUS DA MIGRAÇÃO ---")
    print("✅ RAW: Migrado para S3")
    print("✅ INFRA: Migrado para RDS")
    print("⏳ SILVER: Pendente (Próximo passo)")
    print("⏳ GOLD: Pendente")
    print("--------------------------")

if __name__ == "__main__":
    run_pipeline()