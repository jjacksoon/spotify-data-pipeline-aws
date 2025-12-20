import json
from pathlib import Path

from src.extract.spotify.user_recently_played import get_recently_played
from src.load.raw.raw_loader import save_recently_played_raw
from src.transform.silver.silver_recently_played import run_silver



BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"

def load_access_token() -> str:
    with open(TOKEN_PATH, encoding="utf-8") as f:
        return json.load(f)["access_token"]


'''def run_recently_played():
    token = load_access_token()

    data = get_recently_played(token, limit=10)

    file_path = save_recently_played_raw(data)

    print(f"✅ RAW salvo com sucesso em: {file_path}")'''


def run_pipeline():
    print("🚀 Pipeline iniciado")

    # 1. Extract + Raw
    token = load_access_token()
    data = get_recently_played(token, limit=10)
    raw_path = save_recently_played_raw(data)

    print(f"📥 RAW gerada em {raw_path}")

    # 2. Silver
    run_silver()

    print("✅ Pipeline finalizado com sucesso")

if __name__ == "__main__":
    run_pipeline()
