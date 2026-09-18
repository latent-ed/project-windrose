import requests

def extract_payload(url, params):
    response= requests.get(
        url,
        params,
        timeout=60,
    )

    response.raise_for_status()
    payload = response.json()
    return payload

