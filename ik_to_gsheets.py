import requests
import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth

# --- CONFIGURATION ---
IK_PRIVATE_KEY = "your_imagekit_private_key"
GOOGLE_JSON_KEY = "service_account.json"  # Path to your Google Cloud credentials
TARGET_FOLDER_ID = "13h1eg_GZpfU2K-WjecGdMgNRT0VxTRXI"
SHEET_NAME = "ImageKit_Export_Final"

def export_to_google_sheets():
    # 1. Fetch Data from ImageKit
    print("Fetching data from ImageKit...")
    response = requests.get(
        "https://api.imagekit.io/v1/files",
        auth=HTTPBasicAuth(IK_PRIVATE_KEY, "")
    )
    
    if response.status_code != 200:
        print(f"Error fetching from ImageKit: {response.text}")
        return

    files = response.json()
    rows = []
    for f in files:
        meta = f.get("customMetadata", {})
        rows.append({
            "product_name": meta.get("product_name", ""),
            "product_description": meta.get("description", ""),
            "price": meta.get("price", ""),
            "imagekit_url": f.get("url", ""),
            "file_name": f.get("name", ""),
            "file_id": f.get("fileId", "")
        })

    df = pd.DataFrame(rows)

    # 2. Authenticate with Google Sheets
    print("Authenticating with Google...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON_KEY, scope)
    client = gspread.authorize(creds)

    # 3. Create/Access the Sheet in the specific folder
    try:
        # Create a new spreadsheet
        sh = client.create(SHEET_NAME, folder_id=TARGET_FOLDER_ID)
        worksheet = sh.get_worksheet(0)
        print(f"Created new sheet in folder: {sh.url}")
    except Exception as e:
        print(f"Folder access error (Ensure service account has access): {e}")
        # Fallback: try to open if it already exists
        sh = client.open(SHEET_NAME)
        worksheet = sh.get_worksheet(0)

    # 4. Push DataFrame to Sheet
    # Clear existing content and update with header + values
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    print(f"Successfully pushed {len(df)} rows to Google Sheets.")

if __name__ == "__main__":
    export_to_google_sheets()