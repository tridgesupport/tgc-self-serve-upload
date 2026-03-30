import requests
import os
import pandas as pd
import re
import gdown
from urllib.parse import urlparse

# --- CONFIGURATION ---
BASE_ASSETS_PATH = '/Users/hardik.parikh/Documents/Assets'
INPUT_FILE = 'domains.txt'
ERROR_LOG = 'scraping_errors.log'
# ---------------------

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
}

def clean_html(raw_html):
    if not raw_html: return ""
    cleantext = re.sub('<.*?>', ' ', str(raw_html))
    return ' '.join(cleantext.split())

def get_company_name(url):
    if 'drive.google.com' in url:
        return "GDrive_Download"
    domain = urlparse(url).netloc or url.split('/')[0]
    return domain.replace('www.', '').split('.')[0].strip()

def scrape_shopify(root_url, company_folder):
    json_url = f"{root_url.rstrip('/')}/products.json"
    print(f"   --> Trying Shopify JSON...")
    res = requests.get(json_url, headers=HEADERS, timeout=15)
    
    if res.status_code != 200:
        return []

    products = res.json().get('products', [])
    data = []
    for p in products[:10]:
        item = {
            "Product Name": p.get('title'),
            "Price (INR)": p['variants'][0].get('price', '0'),
            "Description": clean_html(p.get('body_html', ''))
        }
        # Download Images
        images = p.get('images', [])
        for i, img in enumerate(images[:3]):
            try:
                img_res = requests.get(img['src'], stream=True)
                ext = img['src'].split('.')[-1].split('?')[0] or 'jpg'
                img_path = os.path.join(company_folder, f"{get_company_name(p['title'])}_{i}.{ext}")
                with open(img_path, 'wb') as f:
                    f.write(img_res.content)
                item[f"Asset{i+1}_Path"] = img_path
            except: continue
        data.append(item)
    return data

def scrape_wordpress(root_url, company_folder):
    # Try two different WooCommerce API versions
    endpoints = [
        f"{root_url.rstrip('/')}/wp-json/wc/store/v1/products",
        f"{root_url.rstrip('/')}/wp-json/wc/v3/products"
    ]
    
    for api_url in endpoints:
        print(f"   --> Trying WordPress API: {api_url}")
        try:
            res = requests.get(api_url, headers=HEADERS, timeout=15)
            if res.status_code == 200:
                products = res.json()
                data = []
                for p in products[:10]:
                    # Standard WC format uses 'name' and 'images' list
                    item = {
                        "Product Name": p.get('name') or p.get('title', {}).get('rendered'),
                        "Price (INR)": p.get('prices', {}).get('price', '0'),
                        "Description": clean_html(p.get('description') or p.get('excerpt', {}).get('rendered'))
                    }
                    data.append(item)
                return data
        except: continue
    return []

def scrape_gdrive(url, company_folder):
    print(f"   --> Attempting specialized GDrive extraction...")
    try:
        # Use gdown to fuzzy-match the folder content
        file_paths = gdown.download_folder(url, output=company_folder, quiet=False, remaining_ok=True, use_cookies=False)
        
        data = []
        if file_paths:
            for path in file_paths:
                if path and os.path.isfile(path) and path.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
                    data.append({
                        "Product Name": os.path.basename(path),
                        "Price (INR)": "TBD",
                        "Description": "Manual Check Required - GDrive File",
                        "Asset1_Path": path
                    })
            return data
    except Exception as e:
        print(f"   --> GDrive Error: {e}")
    return []

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Missing {INPUT_FILE}"); return

    with open(INPUT_FILE, 'r') as f:
        urls = [l.strip() for l in f if l.strip()]

    for url in urls:
        full_url = url if url.startswith('http') else f"https://{url}"
        c_name = get_company_name(full_url)
        c_folder = os.path.join(BASE_ASSETS_PATH, c_name)
        if not os.path.exists(c_folder): os.makedirs(c_folder)

        print(f"\n--- Processing: {c_name} ---")
        
        results = []
        if 'drive.google.com' in full_url:
            results = scrape_gdrive(full_url, c_folder)
        else:
            # Detect by trying both
            results = scrape_shopify(full_url, c_folder)
            if not results:
                results = scrape_wordpress(full_url, c_folder)

        if results:
            df = pd.DataFrame(results)
            # Ensure standard columns exist
            for col in ["Product Name", "Price (INR)", "Description"]:
                if col not in df.columns: df[col] = "N/A"
            
            df.to_excel(os.path.join(c_folder, f"{c_name}_catalog.xlsx"), index=False)
            print(f"SUCCESS: Saved {len(results)} items.")
        else:
            print(f"FAILED: No data could be extracted from {full_url}")

if __name__ == "__main__":
    main()