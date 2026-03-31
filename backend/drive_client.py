import csv
import io
import json
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

PARENT_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "13h1eg_GZpfU2K-WjecGdMgNRT0VxTRXI")

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------------------------------------------------------------------
# Credentials — user OAuth preferred, service account as fallback
# ---------------------------------------------------------------------------

def _get_user_credentials():
    """
    Build OAuth2 credentials from the three GOOGLE_* env vars.
    Returns None if any of the vars are missing (caller falls back to SA).
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request as AuthRequest

    client_id     = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()

    if not (client_id and client_secret and refresh_token):
        return None

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=DRIVE_SCOPES,
    )
    creds.refresh(AuthRequest())
    return creds


def _get_sa_credentials():
    """Service account credentials — fallback when OAuth not configured."""
    creds_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_str:
        raise EnvironmentError(
            "Neither GOOGLE_REFRESH_TOKEN nor GOOGLE_SERVICE_ACCOUNT_JSON is set."
        )
    creds_info = json.loads(creds_str)
    return service_account.Credentials.from_service_account_info(
        creds_info, scopes=DRIVE_SCOPES
    )


def _best_credentials():
    """Return user OAuth creds if configured, otherwise service account creds."""
    creds = _get_user_credentials()
    if creds is not None:
        return creds
    return _get_sa_credentials()


def _drive():
    return build("drive", "v3", credentials=_best_credentials())


def _sheets():
    return build("sheets", "v4", credentials=_best_credentials())


# ---------------------------------------------------------------------------
# Folder management
# ---------------------------------------------------------------------------

def create_brand_folder(brand: str, timestamp: str) -> tuple[str, str, str]:
    """
    Create a folder named '{brand}_{timestamp}' inside the parent Drive folder.
    Returns (folder_id, web_view_link, folder_name).
    """
    folder_name = f"{brand}_{timestamp}"
    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [PARENT_FOLDER_ID],
    }
    folder = _drive().files().create(
        body=metadata, fields="id,webViewLink", supportsAllDrives=True
    ).execute()
    return folder["id"], folder["webViewLink"], folder_name


# ---------------------------------------------------------------------------
# File uploads
# ---------------------------------------------------------------------------

def upload_media_bytes(
    data: bytes, filename: str, folder_id: str, mime_type: str = "image/jpeg"
) -> str:
    """Upload raw bytes to a Drive folder. Returns the webViewLink."""
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type)
    f = _drive().files().create(
        body=metadata, media_body=media, fields="webViewLink", supportsAllDrives=True
    ).execute()
    return f["webViewLink"]


def upload_csv_to_drive(
    rows: list[dict], headers: list[str], filename: str, folder_id: str
) -> str:
    """Build a CSV from rows/headers and upload it to Drive. Returns webViewLink."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    csv_bytes = buf.getvalue().encode("utf-8")
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv")
    f = _drive().files().create(
        body=metadata, media_body=media, fields="webViewLink", supportsAllDrives=True
    ).execute()
    return f["webViewLink"]


# ---------------------------------------------------------------------------
# Google Sheets — read vendor upload sheet
# ---------------------------------------------------------------------------

def read_sheet_data(spreadsheet_url: str) -> tuple[list[dict], str | None]:
    """
    Read all rows from a Google Sheet.
    First tries the public CSV export URL (works for 'Anyone with the link' sheets).
    Falls back to the Sheets API with service account / user creds.
    Returns (rows, error_detail).  Each row is a dict keyed by normalised
    header name (lowercase, spaces → underscores).
    """
    import re as _re
    import csv as _csv
    import requests as _requests

    m = _re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", spreadsheet_url)
    if not m:
        return [], "Could not extract a spreadsheet ID from that URL."

    spreadsheet_id = m.group(1)

    # --- Try public CSV export first (no auth required) ---
    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
    try:
        resp = _requests.get(csv_url, timeout=30, allow_redirects=True)
        if resp.status_code == 200 and resp.content:
            text = resp.content.decode("utf-8-sig")  # strip BOM if present
            reader = _csv.DictReader(text.splitlines())
            raw_headers = reader.fieldnames or []
            headers = [
                h.strip().lower().replace(" ", "_").replace("-", "_")
                for h in raw_headers
            ]
            rows = []
            for raw_row in reader:
                normalised = {
                    h.strip().lower().replace(" ", "_").replace("-", "_"): v
                    for h, v in raw_row.items()
                }
                if not any(v.strip() for v in normalised.values()):
                    continue
                rows.append(normalised)
            if rows:
                return rows, None
    except Exception:
        pass  # fall through to Sheets API

    # --- Fallback: Sheets API ---
    try:
        svc    = _sheets()
        result = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:Z5000",
        ).execute()
    except Exception as exc:
        msg = str(exc).lower()
        if "not found" in msg or "404" in msg:
            return [], "Spreadsheet not found. Check the URL."
        if "permission" in msg or "403" in msg:
            return [], (
                "Access denied. Share the sheet as 'Anyone with the link' "
                "or add the service account as a Viewer."
            )
        return [], str(exc)

    values = result.get("values", [])
    if not values:
        return [], "The sheet appears to be empty."

    raw_headers = values[0]
    headers = [
        h.strip().lower().replace(" ", "_").replace("-", "_")
        for h in raw_headers
    ]

    rows = []
    for raw_row in values[1:]:
        if not any(c.strip() for c in raw_row):
            continue
        padded = raw_row + [""] * max(0, len(headers) - len(raw_row))
        rows.append(dict(zip(headers, padded)))

    return rows, None


# ---------------------------------------------------------------------------
# Google Sheets export — persistent ImageKit library
# ---------------------------------------------------------------------------

IMAGEKIT_SHEET_NAME = "ImageKit_Library"
IMAGEKIT_HEADERS = [
    "product_name", "description", "price", "brand",
    "level_1", "level_2", "level_3", "level_4", "level_5",
    "imagekit_url", "file_name", "file_id",
]


def _find_sheet(drive_svc, name: str, folder_id: str) -> str | None:
    """Return the spreadsheet ID if a sheet with this name exists in folder_id."""
    result = drive_svc.files().list(
        q=(
            f"'{folder_id}' in parents"
            f" and name='{name}'"
            f" and mimeType='application/vnd.google-apps.spreadsheet'"
            f" and trashed=false"
        ),
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=5,
    ).execute()
    files = result.get("files", [])
    return files[0]["id"] if files else None


def append_to_imagekit_sheet(rows: list[dict], folder_id: str) -> str:
    """
    Append rows to the persistent 'ImageKit_Library' sheet in folder_id.
    Creates the sheet with a header row if it doesn't exist yet.
    Returns the sheet's webViewLink.
    """
    drive_svc  = _drive()
    sheets_svc = _sheets()

    sheet_id = _find_sheet(drive_svc, IMAGEKIT_SHEET_NAME, folder_id)

    if not sheet_id:
        file_meta = {
            "name": IMAGEKIT_SHEET_NAME,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        sheet_file = drive_svc.files().create(
            body=file_meta, fields="id,webViewLink", supportsAllDrives=True
        ).execute()
        sheet_id = sheet_file["id"]
        sheets_svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body={"values": [IMAGEKIT_HEADERS]},
        ).execute()

    if rows:
        values = [[str(row.get(h, "")) for h in IMAGEKIT_HEADERS] for row in rows]
        sheets_svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    link = drive_svc.files().get(
        fileId=sheet_id, fields="webViewLink", supportsAllDrives=True
    ).execute()
    return link["webViewLink"]
