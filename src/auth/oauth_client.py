import os
import base64
import requests
from urllib.parse import urlencode


class OAuthClient:
    def __init__(
        self,
        auth_url: str,
        token_url: str,
        client_id_env: str = "SPOTIFY_CLIENT_ID",
        client_secret_env: str = "SPOTIFY_CLIENT_SECRET",
        redirect_uri_env: str = "SPOTIFY_REDIRECT_URI",
        scope_env: str = "SPOTIFY_SCOPE",
    ):
        self.auth_url = auth_url
        self.token_url = token_url
        self.client_id = os.getenv(client_id_env)
        self.client_secret = os.getenv(client_secret_env)
        self.redirect_uri = os.getenv(redirect_uri_env)
        self.scope = os.getenv(scope_env)

        self._validate_envs()

    def _validate_envs(self):
        missing = [
            name
            for name, value in {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": self.redirect_uri,
            }.items()
            if not value
        ]

        if missing:
            raise EnvironmentError(
                f"Variáveis de ambiente obrigatorias ausentes: {', '.join(missing)}"
            )

    # ==========================
    # Autorização da url
    # ==========================
    def get_authorization_url(self, extra_params: dict | None = None) -> str:
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
        }

        if self.scope:
            params["scope"] = self.scope

        if extra_params:
            params.update(extra_params)

        return f"{self.auth_url}?{urlencode(params)}"

    # ==========================
    # Token
    # ==========================
    def _get_basic_auth_header(self) -> str:
        auth_str = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(auth_str.encode()).decode()

    def exchange_code_for_token(self, code: str) -> dict:
        headers = {
            "Authorization": f"Basic {self._get_basic_auth_header()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }

        response = requests.post(self.token_url, headers=headers, data=data)

        response.raise_for_status()
        return response.json()

    # ==========================
    # Refresh token
    # ==========================
    def refresh_access_token(self, refresh_token: str) -> dict:
        headers = {
            "Authorization": f"Basic {self._get_basic_auth_header()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        response = requests.post(self.token_url, headers=headers, data=data)
        response.raise_for_status()

        return response.json()
