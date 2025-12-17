import json
import requests
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"


def load_access_token():
    with open(TOKEN_PATH) as f:
        return json.load(f)["access_token"]


def run():
    token = load_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(
        "https://api.spotify.com/v1/me/top/artists?limit=10",
        headers=headers
    )

    print("Status:", response.status_code)
    print("-" * 50)

    data = response.json()

    for i, artist in enumerate(data["items"], start=1):
        print(f"{i}. {artist['name']}")
        print(f"   Popularidade: {artist['popularity']}")
        print(f"   Seguidores: {artist['followers']['total']}")
        print(f"   Gêneros: {', '.join(artist['genres']) if artist['genres'] else 'N/A'}")
        print("-" * 50)


if __name__ == "__main__":
    run()
