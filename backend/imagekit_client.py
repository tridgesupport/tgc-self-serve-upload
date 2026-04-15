import json
import os

import requests
from requests.auth import HTTPBasicAuth

IK_PRIVATE_KEY = os.environ.get("IMAGEKIT_PRIVATE_KEY", "")
UPLOAD_URL = "https://upload.imagekit.io/api/v1/files/upload"
API_URL = "https://api.imagekit.io/v1/files"


def upload_to_imagekit(
    image_url: str,
    filename: str,
    folder: str,
    metadata: dict,
) -> dict | None:
    """
    Download image_url and upload it to ImageKit with custom metadata.
    Returns the ImageKit response dict on success, None on failure.
    """
    if not IK_PRIVATE_KEY:
        print("[ImageKit] IMAGEKIT_PRIVATE_KEY is not set — skipping upload")
        return None

    try:
        dl = requests.get(image_url, timeout=30, allow_redirects=True)
        dl.raise_for_status()
        content_type = dl.headers.get("content-type", "image/jpeg").split(";")[0].strip()

        if not dl.content:
            print(f"[ImageKit] Downloaded 0 bytes from {image_url}")
            return None

        # Strip empty strings — ImageKit rejects empty values for defined fields
        # Truncate product_description to ImageKit's 1024-char metadata limit
        if "product_description" in metadata and metadata["product_description"]:
            metadata["product_description"] = metadata["product_description"][:1024]
        metadata_clean = {k: v for k, v in metadata.items() if v != "" and v is not None}

        resp = requests.post(
            UPLOAD_URL,
            auth=HTTPBasicAuth(IK_PRIVATE_KEY, ""),
            data={
                "fileName": filename,
                "folder": f"/{folder}",
                "customMetadata": json.dumps(metadata_clean),
                "useUniqueFileName": "false",
            },
            files={"file": (filename, dl.content, content_type)},
            timeout=60,
        )

        if resp.status_code == 200:
            return resp.json()

        print(f"[ImageKit] Upload failed ({resp.status_code}) for {filename}: {resp.text}")
        return None

    except requests.exceptions.HTTPError as exc:
        print(f"[ImageKit] Download HTTP error for {image_url}: {exc}")
        return None
    except Exception as exc:
        print(f"[ImageKit] Unexpected error for {filename}: {exc}")
        return None


def fetch_all_imagekit_files() -> list[dict]:
    """
    Fetch every file from ImageKit (paginated).
    Returns a list of file objects (each includes customMetadata).
    """
    all_files: list[dict] = []
    skip = 0
    limit = 100

    while True:
        resp = requests.get(
            API_URL,
            auth=HTTPBasicAuth(IK_PRIVATE_KEY, ""),
            params={"limit": limit, "skip": skip},
            timeout=30,
        )

        if resp.status_code != 200:
            print(f"ImageKit fetch error ({resp.status_code}): {resp.text}")
            break

        files = resp.json()
        if not files:
            break

        all_files.extend(files)

        if len(files) < limit:
            break

        skip += limit

    return all_files
