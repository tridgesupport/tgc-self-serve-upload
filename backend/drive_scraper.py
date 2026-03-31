"""
Google Drive folder/file scraper.

Strategy (in order):
  1. Drive API (service account) — list + download files directly.
     Works when the service account has been granted access OR when the
     folder is shared with the service account email.
  2. gdown fallback — for fully public "anyone with the link" folders that
     the service account cannot see via the API.

Returns product records with thumbnail URLs pointing to uploaded copies in
the destination Drive folder.
"""

import io
import mimetypes
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp", ".heic"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".avi", ".m4v", ".mkv"}
MEDIA_EXTS  = IMAGE_EXTS | VIDEO_EXTS

BATCH_SIZE  = 20   # upload this many files before pausing
BATCH_PAUSE = 3    # seconds between batches
MAX_FILES   = 200  # safety cap so we don't accidentally download huge folders


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

    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "folder"

    m = re.search(r"/file/d/([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    m = re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    m = re.search(r"open\?id=([a-zA-Z0-9_-]{10,})", url)
    if m:
        return m.group(1), "file"

    return None, "invalid"


# ---------------------------------------------------------------------------
# Drive API helpers
# ---------------------------------------------------------------------------

def _list_files_in_folder(drive_svc, folder_id: str) -> list[dict]:
    """
    List all non-trashed files directly inside folder_id using the Drive API.
    Raises on permission / not-found errors so the caller can decide the status.
    """
    files = []
    page_token = None
    while True:
        kwargs = dict(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=100,
        )
        if page_token:
            kwargs["pageToken"] = page_token
        resp = drive_svc.files().list(**kwargs).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token or len(files) >= MAX_FILES:
            break
    return files[:MAX_FILES]


def _download_file_api(drive_svc, file_id: str, dest_path: str) -> None:
    """Download a single Drive file to dest_path using the API."""
    from googleapiclient.http import MediaIoBaseDownload
    request = drive_svc.files().get_media(
        fileId=file_id, supportsAllDrives=True
    )
    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _get_single_file_meta(drive_svc, file_id: str) -> dict | None:
    """Return name + mimeType for a single file ID, or None on error."""
    try:
        return drive_svc.files().get(
            fileId=file_id,
            fields="id,name,mimeType",
            supportsAllDrives=True,
        ).execute()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# gdown fallback
# ---------------------------------------------------------------------------

def _gdown_folder(folder_id: str, out_dir: str) -> tuple[list[str] | None, str | None]:
    """
    Try to download a public folder with gdown.
    Returns (paths, error_detail).
    paths=None means access denied; paths=[] means empty; error_detail set on failure.
    """
    try:
        import gdown
        paths = gdown.download_folder(
            id=folder_id,
            output=out_dir,
            quiet=True,
            remaining_ok=True,
            use_cookies=False,
        )
        return (paths or []), None
    except Exception as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("private", "permission", "access denied", "may still be", "cannot retrieve")):
            return None, str(exc)
        return None, str(exc)


def _gdown_file(file_id: str, out_dir: str) -> tuple[list[str] | None, str | None]:
    """Try to download a single public file with gdown."""
    try:
        import gdown
        path = gdown.download(
            id=file_id,
            output=os.path.join(out_dir, file_id),
            quiet=True,
            use_cookies=False,
            fuzzy=True,
        )
        return ([path] if path else []), None
    except Exception as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_drive(
    url: str, brand: str, dest_folder_id: str
) -> tuple[list[dict], str, str | None, str | None]:
    """
    Download media from a public Drive URL, upload copies to dest_folder_id,
    and return product records.

    Returns:
        (products, status, drive_folder_url, error_detail)
        status: 'ok' | 'invalid_url' | 'not_public' | 'empty' | 'error'
        error_detail: human-readable message for non-ok statuses
    """
    from drive_client import _drive, create_brand_folder

    drive_id, id_type = extract_drive_id(url)
    if not drive_id:
        return [], "invalid_url", None, None

    tmp_dir = tempfile.mkdtemp(prefix="tgc_gdrive_")
    try:
        drive_svc = _drive()

        # ── Step 1: collect local file paths ────────────────────────────────
        media_files: list[str] = []  # list of (local_path, original_name) tuples
        error_detail: str | None = None
        used_api = False

        if id_type == "folder":
            # Try Drive API first
            try:
                api_files = _list_files_in_folder(drive_svc, drive_id)
                # filter to media
                for f in api_files:
                    ext = Path(f["name"]).suffix.lower()
                    if ext in MEDIA_EXTS:
                        dest = os.path.join(tmp_dir, f["name"])
                        _download_file_api(drive_svc, f["id"], dest)
                        media_files.append(dest)
                used_api = True
                print(f"[DriveScaper] API listed {len(api_files)} files, {len(media_files)} media")
            except Exception as api_exc:
                api_err = str(api_exc)
                print(f"[DriveScaper] API list failed ({api_err}), falling back to gdown")
                # Fall back to gdown
                paths, gdown_err = _gdown_folder(drive_id, tmp_dir)
                if paths is None:
                    # Check if it's a permission issue
                    err_lower = (gdown_err or "").lower()
                    if any(k in err_lower for k in ("private", "permission", "access denied", "cannot retrieve")):
                        return [], "not_public", None, (
                            f"This folder is not accessible. Make sure it is set to "
                            f"'Anyone with the link can view', or share it with the service account. "
                            f"Detail: {gdown_err}"
                        )
                    return [], "error", None, (
                        f"Could not access the Drive folder via API or gdown.\n"
                        f"API error: {api_err}\n"
                        f"gdown error: {gdown_err}"
                    )
                media_files = [
                    p for p in paths
                    if p and os.path.isfile(p) and Path(p).suffix.lower() in MEDIA_EXTS
                ]

        else:
            # Single file — try API first
            try:
                meta = _get_single_file_meta(drive_svc, drive_id)
                if meta:
                    ext = Path(meta["name"]).suffix.lower()
                    if ext in MEDIA_EXTS:
                        dest = os.path.join(tmp_dir, meta["name"])
                        _download_file_api(drive_svc, drive_id, dest)
                        media_files = [dest]
                    used_api = True
            except Exception as api_exc:
                print(f"[DriveScaper] API single-file failed ({api_exc}), falling back to gdown")
                paths, gdown_err = _gdown_file(drive_id, tmp_dir)
                if paths is None:
                    return [], "error", None, (
                        f"Could not download this file.\n"
                        f"API error: {api_exc}\n"
                        f"gdown error: {gdown_err}"
                    )
                media_files = [
                    p for p in paths
                    if p and os.path.isfile(p) and Path(p).suffix.lower() in MEDIA_EXTS
                ]

        if not media_files:
            return [], "empty", None, "No image or video files were found at that Drive URL."

        # ── Step 2: create destination folder ────────────────────────────────
        import re as _re
        from datetime import datetime
        brand_safe = _re.sub(r"[^\w\-]", "_", brand).strip("_")
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_folder_id_new, dest_folder_url, _ = create_brand_folder(brand_safe, timestamp)

        # ── Step 3: upload to destination in batches ─────────────────────────
        products = []
        for i, local_path in enumerate(media_files):
            ext       = Path(local_path).suffix.lower()
            asset_type = "video" if ext in VIDEO_EXTS else "image"
            mime      = mimetypes.guess_type(local_path)[0] or (
                "video/mp4" if asset_type == "video" else "image/jpeg"
            )
            filename = Path(local_path).name

            with open(local_path, "rb") as fh:
                data = fh.read()

            from googleapiclient.http import MediaIoBaseUpload
            file_meta   = {"name": filename, "parents": [dest_folder_id_new]}
            media_upload = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime)
            uploaded    = drive_svc.files().create(
                body=file_meta, media_body=media_upload, fields="id"
            ).execute()
            file_id = uploaded["id"]

            # Make publicly readable for thumbnail preview
            drive_svc.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()

            thumbnail_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w800"
            products.append({
                "product_name": "",
                "price": "",
                "description": "",
                "assets": [{"url": thumbnail_url, "type": asset_type}],
            })

            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < len(media_files):
                time.sleep(BATCH_PAUSE)

        return products, "ok", dest_folder_url, None

    except Exception as exc:
        import traceback
        detail = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        print(f"[DriveScaper] Unexpected error:\n{detail}")
        return [], "error", None, str(exc)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
