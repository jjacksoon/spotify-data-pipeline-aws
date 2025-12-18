from flask import Flask, redirect, request
from dotenv import load_dotenv
from pathlib import Path
from oauth_client import OAuthClient
import json

# ==========================
# Load .env 
# ==========================
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

app = Flask(__name__)

oauth = OAuthClient(
    auth_url="https://accounts.spotify.com/authorize",
    token_url="https://accounts.spotify.com/api/token",
)

# ==========================
# Login
# ==========================
@app.route("/")
def login():
    auth_url = oauth.get_authorization_url(
        extra_params={"show_dialog": "true"}
    )
    return redirect(auth_url)

# ==========================
# Callback
# ==========================
@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Erro: authorization code não recebido", 400
    token = oauth.exchange_code_for_token(code)
    
    with open("token.json","w") as f:
        json.dump(token, f, indent = 2)
    
    return 'Login ok! Token salvo'

if __name__ == "__main__":
    app.run(host = "127.0.0.1", port = 8000, debug = True)