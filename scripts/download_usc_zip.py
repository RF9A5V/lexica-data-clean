import os
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
from tqdm import tqdm

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/raw'))
XML_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/xml'))
ZIP_OUTPUT_PATH = os.path.join(RAW_DIR, 'usc.zip')
DOWNLOAD_PAGE_URL = 'https://uscode.house.gov/download/download.shtml'


def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(XML_DIR, exist_ok=True)

def get_bulk_xml_url():
    print('Fetching download page...')
    resp = requests.get(DOWNLOAD_PAGE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    link = soup.find('a', title='All USC Titles in XML')
    if not link or not link.get('href'):
        print('Could not find <a> tag with title="All USC Titles in XML". Dumping a snippet of HTML:')
        print(resp.text[:2000])
        raise RuntimeError('Could not find XML ZIP link on download page')
    href = link['href']
    url = href if href.startswith('http') else f'https://uscode.house.gov{href}'
    print('Found bulk XML ZIP URL:', url)
    return url

def download_file(url, output_path):
    print(f'Downloading {url} to {output_path}...')
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        with open(output_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc='Downloading ZIP') as pbar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
    print('Download completed successfully.')

def unzip_to_xml_dir(zip_path, out_dir):
    print(f'Unzipping {zip_path} to {out_dir}...')
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(out_dir)
    print('Unzip complete.')

def main():
    ensure_dirs()
    url = get_bulk_xml_url()
    download_file(url, ZIP_OUTPUT_PATH)
    unzip_to_xml_dir(ZIP_OUTPUT_PATH, XML_DIR)
    print('USC XML download and extraction complete.')

if __name__ == '__main__':
    main()
