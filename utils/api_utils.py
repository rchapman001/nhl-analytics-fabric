import requests
import time


def get_json(
    url: str,
    params: dict | None = None,
    timeout: int = 30,
    retries: int = 3,
    retry_delay: int = 2,
):
    for attempt in range(retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 429:
                if attempt == retries:
                    response.raise_for_status()

                time.sleep(
                    retry_delay * (attempt + 1)
                )

                continue

            response.raise_for_status()

            return response.json()

        except requests.RequestException:
            if attempt == retries:
                raise

            time.sleep(
                retry_delay * (attempt + 1)
            )

    raise RuntimeError(
        f"Failed to retrieve data from {url}"
    )