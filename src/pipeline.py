import json
import requests
from pathlib import Path
from extract.user_recently_played import get_recently_played

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"


def load_access_token():
    with open(TOKEN_PATH) as f:
        return json.load(f)["access_token"]

#Pegando músicas ouvidas recentemente
def run_recently_played():
    token = load_access_token()

    data, status_code = get_recently_played(token, 10)
    try:
        if status_code == 200:
            print(data)
        else:
            raise Exception(f"Requisição não foi bem sucedida. Status code retornado: {status_code}")
    except Exception as e:
        print("Erro na requisição", e)

if __name__ == "__main__":
    run_recently_played()
