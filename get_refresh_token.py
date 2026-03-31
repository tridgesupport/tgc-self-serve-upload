"""
One-time script to get a Google OAuth2 refresh token for Drive access.

Run this locally (NOT on Render):
    pip install google-auth-oauthlib
    python get_refresh_token.py

Then copy the three values printed at the end into Render environment variables:
    GOOGLE_CLIENT_ID
    GOOGLE_CLIENT_SECRET
    GOOGLE_REFRESH_TOKEN
"""

import json

CLIENT_ID     = input("Paste your OAuth2 Client ID:     ").strip()
CLIENT_SECRET = input("Paste your OAuth2 Client Secret: ").strip()

from google_auth_oauthlib.flow import InstalledAppFlow

client_config = {
    "installed": {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
        "token_uri":     "https://oauth2.googleapis.com/token",
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
    }
}

flow = InstalledAppFlow.from_client_config(
    client_config,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)

creds = flow.run_local_server(port=0)

print("\n" + "="*60)
print("Add these to Render → Environment:")
print("="*60)
print(f"GOOGLE_CLIENT_ID     = {CLIENT_ID}")
print(f"GOOGLE_CLIENT_SECRET = {CLIENT_SECRET}")
print(f"GOOGLE_REFRESH_TOKEN = {creds.refresh_token}")
print("="*60)
