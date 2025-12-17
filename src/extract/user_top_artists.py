import requests


def get_user_top_artists(access_token: str, limit: int = 20) -> dict:
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(
        "https://api.spotify.com/v1/me/top/artists",
        headers=headers,
        params={"limit": limit}
    )

    response.raise_for_status()
    return response.json()
