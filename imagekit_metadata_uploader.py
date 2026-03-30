# ... (Keep existing imports and config)

def upload_file_with_meta(local_path, folder, filename, private_key, metadata_dict):
    """Uploads file and attaches product info as ImageKit metadata."""
    with open(local_path, "rb") as f:
        response = requests.post(
            "https://upload.imagekit.io/api/v1/files/upload",
            auth=HTTPBasicAuth(private_key, ""),
            data={
                "fileName": filename,
                "folder": f"/{folder}",
                "customMetadata": json.dumps(metadata_dict), # Attaches metadata here
                "useUniqueFileName": "false"
            },
            files={"file": f},
            timeout=60
        )
    return response.json() if response.status_code == 200 else None

# In main loop:
metadata = {
    "price": str(row_data['price']),
    "description": str(row_data['product_description']),
    "product_name": str(row_data['product_name'])
}
# upload_file_with_meta(path, folder, filename, key, metadata)