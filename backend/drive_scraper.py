"""
Google Drive folder/file scraper.

Strategy:
  1. Use the Drive API (service account) to list files in the source folder.
     Returns thumbnail URLs directly from source file IDs — NO re-upload needed,
     since the files are already in Google Drive.
  2. If the Drive API can't list the folder (permission denied), fall back to
     gdown to download files, then re-upload them with supportsAllDrives=True
     so they land in the Shared Drive (not the service account's personal storage).

Public "anyone with the link" folders are accessible to the service account for
listing/reading. The storage quota error only occurs when uploading to personal
Drive — the API path avoids this entirely.
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

BATCH_SIZE  = 20   # upload this many files before pausing (gdown fallback path)
BATCH_PAUSE = 3    # seconds between batches
MAX_FILES   = 200  # safety cap


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def extract_drive_id(url: str) -> tuple[str | None, str]:
    """
    Returns (id, type) where type is 'folder', 'file', or 'invalid'.
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
    """List all non-trashed media files directly inside folder_id."""
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


def _get_file_meta(drive_svc, file_id: str) -> dict | None:
    """Return id, name, mimeType for a single file ID."""
    return drive_svc.files().get(
        fileId=file_id,
        fields="id,name,mimeType",
        supportsAllDrives=True,
    ).execute()


# ---------------------------------------------------------------------------
# gdown fallback helpers
# ---------------------------------------------------------------------------

def _gdown_download_folder(folder_id: str, out_dir: str) -> tuple[list[str] | None, str | None]:
    """Download public folder with gdown. Returns (paths, error_detail)."""
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
        return None, str(exc)


def _gdown_download_file(file_id: str, out_dir: str) -> tuple[list[str] | None, str | None]:
    """Download a single public file with gdown."""
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


def _upload_to_shared_drive(drive_svc, data: bytes, filename: str, mime: str, folder_id: str) -> str:
    """
    Upload bytes to a Shared Drive folder.
    Uses supportsAllDrives=True so the file goes into the Shared Drive,
    NOT into the service account's personal (zero-quota) storage.
    """
    from googleapiclient.http import MediaIoBaseUpload
    file_meta    = {"name": filename, "parents": [folder_id]}
    media_upload = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime)
    uploaded = drive_svc.files().create(
        body=file_meta,
        media_body=media_upload,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    file_id = uploaded["id"]

    # Make publicly readable so the browser can load the thumbnail
    drive_svc.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    return file_id


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_drive(
    url: str, brand: str, dest_folder_id: str
) -> tuple[list[dict], str, str | None, str | None]:
    """
    Scrape media from a Drive URL and return product records.

    Returns:
        (products, status, drive_folder_url, error_detail)
        status: 'ok' | 'invalid_url' | 'not_public' | 'empty' | 'error'
    """
    from drive_client import _drive, create_brand_folder

    drive_id, id_type = extract_drive_id(url)
    if not drive_id:
        return [], "invalid_url", None, None

    try:
        drive_svc = _drive()

        # ── Step 1: collect (file_id, name, asset_type) tuples ─────────────
        # Prefer the API path — no download/re-upload needed, use source IDs directly.
        # Fall back to gdown only if the API can't list the folder.

        api_items: list[dict] = []   # [{id, name, mimeType}]
        gdown_paths: list[str] = []  # local paths (gdown fallback only)
        error_detail: str | None = None
        used_gdown = False

        if id_type == "folder":
            try:
                all_files = _list_files_in_folder(drive_svc, drive_id)
                api_items = [
                    f for f in all_files
                    if Path(f["name"]).suffix.lower() in MEDIA_EXTS
                ]
                print(f"[DriveScaper] API listed {len(all_files)} files, {len(api_items)} media")
            except Exception as api_exc:
                api_err = str(api_exc)
                print(f"[DriveScaper] API list failed: {api_err} — trying gdown")
                tmp_dir = tempfile.mkdtemp(prefix="tgc_gdrive_")
                paths, gdown_err = _gdown_download_folder(drive_id, tmp_dir)
                if paths is None:
                    err_lower = (gdown_err or "").lower()
                    if any(k in err_lower for k in ("private", "permission", "access denied", "cannot retrieve")):
                        return [], "not_public", None, (
                            "This folder is not accessible. Set sharing to "
                            "'Anyone with the link can view' and try again. "
                            f"Detail: {gdown_err}"
                        )
                    return [], "error", None, (
                        f"Drive API error: {api_err}\ngdown error: {gdown_err}"
                    )
                gdown_paths = [
                    p for p in paths
                    if p and os.path.isfile(p) and Path(p).suffix.lower() in MEDIA_EXTS
                ]
                used_gdown = True

        else:  # single file
            try:
                meta = _get_file_meta(drive_svc, drive_id)
                if Path(meta["name"]).suffix.lower() in MEDIA_EXTS:
                    api_items = [meta]
            except Exception as api_exc:
                api_err = str(api_exc)
                print(f"[DriveScaper] API single-file failed: {api_err} — trying gdown")
                tmp_dir = tempfile.mkdtemp(prefix="tgc_gdrive_")
                paths, gdown_err = _gdown_download_file(drive_id, tmp_dir)
                if paths is None:
                    return [], "error", None, (
                        f"Drive API error: {api_err}\ngdown error: {gdown_err}"
                    )
                gdown_paths = [
                    p for p in paths
                    if p and os.path.isfile(p) and Path(p).suffix.lower() in MEDIA_EXTS
                ]
                used_gdown = True

        if not api_items and not gdown_paths:
            return [], "empty", None, "No image or video files were found at that Drive URL."

        # ── Step 2: create destination folder (for the manifest / organisation) ──
        import re as _re
        from datetime import datetime
        brand_safe = _re.sub(r"[^\w\-]", "_", brand).strip("_")
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_folder_id_new, dest_folder_url, _ = create_brand_folder(brand_safe, timestamp)

        # ── Step 3: build product records ───────────────────────────────────
        products = []

        if not used_gdown:
            # ── API path: use source file IDs directly, no re-upload ────────
            for f in api_items:
                ext        = Path(f["name"]).suffix.lower()
                asset_type = "video" if ext in VIDEO_EXTS else "image"
                # Thumbnail URL works for any file the service account can read
                thumbnail_url = f"https://drive.google.com/thumbnail?id={f['id']}&sz=w800"
                products.append({
                    "product_name": "",
                    "price": "",
                    "description": "",
                    "assets": [{"url": thumbnail_url, "type": asset_type}],
                })

        else:
            # ── gdown fallback: upload to Shared Drive with supportsAllDrives ──
            try:
                for i, local_path in enumerate(gdown_paths):
                    ext        = Path(local_path).suffix.lower()
                    asset_type = "video" if ext in VIDEO_EXTS else "image"
                    mime       = mimetypes.guess_type(local_path)[0] or (
                        "video/mp4" if asset_type == "video" else "image/jpeg"
                    )
                    with open(local_path, "rb") as fh:
                        data = fh.read()

                    file_id = _upload_to_shared_drive(
                        drive_svc, data, Path(local_path).name, mime, dest_folder_id_new
                    )
                    thumbnail_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w800"
                    products.append({
                        "product_name": "",
                        "price": "",
                        "description": "",
                        "assets": [{"url": thumbnail_url, "type": asset_type}],
                    })

                    if (i + 1) % BATCH_SIZE == 0 and (i + 1) < len(gdown_paths):
                        time.sleep(BATCH_PAUSE)
            finally:
                if 'tmp_dir' in dir():
                    shutil.rmtree(tmp_dir, ignore_errors=True)

        return products, "ok", dest_folder_url, None

    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        print(f"[DriveScaper] Unexpected error:\n{tb}")
        return [], "error", None, str(exc)
