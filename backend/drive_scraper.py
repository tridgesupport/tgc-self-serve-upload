"""
Google Drive folder/file scraper.

Flow:
  1. Parse the Drive URL to get folder/file ID.
  2. Use gdown to download media files from the public folder to a temp dir.
  3. Upload each file to the destination Drive folder (batched, with pauses to
     avoid rate-limiting).
  4. Make each uploaded file publicly readable so the browser can preview it.
  5. Return product records with thumbnail URLs.

Caller is responsible for nothing — temp dir is cleaned up internally.
"""

import mimetypes
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

import gdown

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp", ".heic"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".avi", ".m4v", ".mkv"}
MEDIA_EXTS = IMAGE_EXTS | VIDEO_EXTS

BATCH_SIZE = 20   # upload this many files before pausing
BATCH_PAUSE = 3   # seconds to sleep between batches


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def extract_drive_id(url: str) -> tuple[str | None, str]:
    """
    Returns (id, type) where type is 'folder', 'file', or 'invalid'.
    Handles all common Drive URL formats.
    """
    if not url:
        return None, "invalid"

    # Folder URLs — /folders/{ID}
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "folder"

    # File URLs — /file/d/{ID}/
    m = re.search(r"/file/d/([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    # Query param — ?id={ID} or &id={ID}
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    # Open URL — drive.google.com/open?id={ID}
    m = re.search(r"open\?id=([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    return None, "invalid"


# ---------------------------------------------------------------------------
# Download helpers (gdown)
# ---------------------------------------------------------------------------

def _download_folder(folder_id: str, out_dir: str) -> list[str] | None:
    """
    Download all files from a public Drive folder using gdown.
    Returns list of local file paths, or None if access is denied.
    """
    try:
        paths = gdown.download_folder(
            id=folder_id,
            output=out_dir,
            quiet=True,
            remaining_ok=True,
            use_cookies=False,
        )
        return paths or []
    except Exception as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("private", "permission", "access denied", "may still be")):
            return None
        raise


def _download_file(file_id: str, out_dir: str) -> list[str] | None:
    """Download a single public Drive file. Returns [path] or None."""
    try:
        path = gdown.download(
            id=file_id,
            output=os.path.join(out_dir, file_id),
            quiet=True,
            use_cookies=False,
            fuzzy=True,
        )
        return [path] if path else []
    except Exception as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("private", "permission", "access denied")):
            return None
        raise


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_drive(url: str, brand: str, dest_folder_id: str) -> tuple[list[dict], str, str | None]:
    """
    Download media from a public Drive URL, upload to dest_folder_id,
    and return product records.

    Returns:
        (products, status, drive_folder_url)
        status: 'ok' | 'invalid_url' | 'not_public' | 'empty' | 'error'
    """
    from drive_client import _drive, create_brand_folder  # lazy — avoids circular import

    drive_id, id_type = extract_drive_id(url)
    if not drive_id:
        return [], "invalid_url", None

    tmp_dir = tempfile.mkdtemp(prefix="tgc_gdrive_")
    try:
        # 1. Download from source
        if id_type == "folder":
            downloaded = _download_folder(drive_id, tmp_dir)
        else:
            downloaded = _download_file(drive_id, tmp_dir)

        if downloaded is None:
            return [], "not_public", None

        # Filter to media files only
        media_files = [
            p for p in (downloaded or [])
            if p and os.path.isfile(p) and Path(p).suffix.lower() in MEDIA_EXTS
        ]

        if not media_files:
            return [], "empty", None

        # 2. Create destination folder in Drive
        from datetime import datetime
        import re as _re
        brand_safe = _re.sub(r"[^\w\-]", "_", brand).strip("_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_folder_id_new, dest_folder_url, _ = create_brand_folder(brand_safe, timestamp)

        drive_svc = _drive()

        # 3. Upload in batches with pauses
        products = []
        for i, local_path in enumerate(media_files):
            ext = Path(local_path).suffix.lower()
            asset_type = "video" if ext in VIDEO_EXTS else "image"
            mime = mimetypes.guess_type(local_path)[0] or (
                "video/mp4" if asset_type == "video" else "image/jpeg"
            )
            filename = Path(local_path).name

            with open(local_path, "rb") as fh:
                data = fh.read()

            from googleapiclient.http import MediaIoBaseUpload
            import io

            file_meta = {"name": filename, "parents": [dest_folder_id_new]}
            media_upload = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime)
            uploaded = drive_svc.files().create(
                body=file_meta, media_body=media_upload, fields="id"
            ).execute()
            file_id = uploaded["id"]

            # Make publicly readable so the browser can load the thumbnail
            drive_svc.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()

            thumbnail_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w800"

            products.append(
                {
                    "product_name": "",
                    "price": "",
                    "description": "",
                    "assets": [{"url": thumbnail_url, "type": asset_type}],
                }
            )

            # Pause after every BATCH_SIZE uploads
            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < len(media_files):
                time.sleep(BATCH_PAUSE)

        return products, "ok", dest_folder_url

    except Exception as exc:
        print(f"[DriveScaper] Unexpected error: {exc}")
        return [], "error", None

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
