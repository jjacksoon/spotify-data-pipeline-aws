import json
import requests
from pathlib import Path
from extract.user_top_artists import get_user_top_artists

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"


def load_access_token():
    with open(TOKEN_PATH) as f:
        return json.load(f)["access_token"]


def run():
    token = load_access_token()
    
    # Chama a função que retorna os dados e o status - top 10 artistas mais ouvidos
    data, status_code = get_user_top_artists(token, 10)
    
    try:
        if status_code == 200:
            print(f"Requisição feita com sucesso: {status_code}\n")

            for i, artist in enumerate(data["items"], start=1):
                print(f"{i}. {artist['name']}")
                print(f"   Popularidade: {artist['popularity']}")
                print(f"   Seguidores: {artist['followers']['total']}")
                print(f"   Gêneros: {', '.join(artist['genres']) if artist['genres'] else 'N/A'}")
                print("-" * 50)
        else:
            # Se o status não for 200, lança uma exceção
            raise Exception(f"Requisição não foi bem sucedida. Status code retornado: {status_code}")

    except Exception as e:
        # Captura a exceção e mostra na tela
        print("Erro na requisição:", e)


if __name__ == "__main__":
    run()
