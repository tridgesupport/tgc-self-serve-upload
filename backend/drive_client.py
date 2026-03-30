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


def _get_credentials():
    creds_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_str:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON environment variable is not set. "
            "Paste the entire service account JSON as a single-line string."
        )
    creds_info = json.loads(creds_str)
    return service_account.Credentials.from_service_account_info(
        creds_info, scopes=DRIVE_SCOPES
    )


def _drive():
    return build("drive", "v3", credentials=_get_credentials())


def _sheets():
    return build("sheets", "v4", credentials=_get_credentials())


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
    folder = _drive().files().create(body=metadata, fields="id,webViewLink").execute()
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
    f = _drive().files().create(body=metadata, media_body=media, fields="webViewLink").execute()
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
    f = _drive().files().create(body=metadata, media_body=media, fields="webViewLink").execute()
    return f["webViewLink"]


# ---------------------------------------------------------------------------
# Google Sheets export
# ---------------------------------------------------------------------------

def create_sheet_in_drive(
    data_rows: list[dict], sheet_name: str, folder_id: str
) -> str:
    """
    Create a Google Sheet inside folder_id, populate it with data_rows,
    and return its webViewLink.
    """
    drive_svc = _drive()
    sheets_svc = _sheets()

    # Create the spreadsheet file in Drive
    file_meta = {
        "name": sheet_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }
    sheet_file = drive_svc.files().create(body=file_meta, fields="id,webViewLink").execute()
    spreadsheet_id = sheet_file["id"]

    if data_rows:
        headers = list(data_rows[0].keys())
        values = [headers] + [
            [str(row.get(h, "")) for h in headers] for row in data_rows
        ]
        sheets_svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

    return sheet_file["webViewLink"]
