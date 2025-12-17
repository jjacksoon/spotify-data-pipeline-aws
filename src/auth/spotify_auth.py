import os
import json
import base64
import requests
from urllib.parse import urlencode
from flask import Flask, redirect, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"


def get_auth_header():
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    auth_str = f"{client_id}:{client_secret}"
    return base64.b64encode(auth_str.encode()).decode()


@app.route("/")
def login():
    scope = os.getenv("SPOTIFY_SCOPE")

    # fallback seguro
    if not scope:
        scope = "user-top-read"

    params = {
        "client_id": os.getenv("SPOTIFY_CLIENT_ID"),
        "response_type": "code",
        "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI"),
        "scope": scope,
        "show_dialog": "true"
    }

    url = f"{AUTH_URL}?{urlencode(params)}"
    return redirect(url)


@app.route("/callback")
def callback():
    code = request.args.get("code")

    if not code:
        return "Erro: code não encontrado", 400

    headers = {
        "Authorization": f"Basic {get_auth_header()}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI")
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)
    response.raise_for_status()

    token_data = response.json()

    with open("token.json", "w") as f:
        json.dump(token_data, f, indent=2)

    return "✅ Token salvo com sucesso! Pode fechar esta aba."


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)
