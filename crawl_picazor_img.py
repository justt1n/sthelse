import cloudscraper
from bs4 import BeautifulSoup
import os
import urllib.parse
import time
import concurrent.futures
import requests  # For type hinting and specific exceptions
from dotenv import load_dotenv  # Import for .env file loading

# --- Load Environment Variables ---
load_dotenv()  # Load variables from .env file into environment

# --- Configuration ---
# Read from environment variables, with defaults if not found
BASE_URL = "https://picazor.com"  # This could also be in .env if it changes often per site
PAGE_URL_TEMPLATE = os.getenv("PAGE_URL_TEMPLATE", "https://picazor.com/en/thanh-nhen/{}")
START_PAGE = int(os.getenv("START_PAGE", "1"))  # Ensure conversion to int
END_PAGE = int(os.getenv("END_PAGE", "568"))  # Ensure conversion to int
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "picazor_thanh_nhen_images_only_default")  # Directory for images

# Concurrency Settings (can also be moved to .env if desired)
MAX_CONCURRENT_SCRAPERS = int(os.getenv("MAX_CONCURRENT_SCRAPERS", "5"))
MAX_CONCURRENT_DOWNLOADERS = int(os.getenv("MAX_CONCURRENT_DOWNLOADERS", "5"))
DOWNLOAD_BATCH_SIZE = int(os.getenv("DOWNLOAD_BATCH_SIZE", "100"))

# Delay Settings (can also be moved to .env if desired)
REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD = int(os.getenv("REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD", "1"))
REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD = int(os.getenv("REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD", "1"))
DELAY_BETWEEN_DOWNLOAD_BATCHES = int(os.getenv("DELAY_BETWEEN_DOWNLOAD_BATCHES", "5"))

# Base headers for the scraper
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    # 'Referer' will be set per request
}


# --- Helper Functions ---
def generate_filename_from_url(url, page_number, media_type_hint="image"):  # Default to image
    """Generates a filename from a URL, page number, and media type hint."""
    try:
        path = urllib.parse.urlparse(url).path
        base_name = os.path.basename(path)
        if not base_name or '.' not in base_name:  # No filename or no extension
            ext = ".jpg"
            parsed_url = urllib.parse.urlparse(url)
            if "_next/image" in parsed_url.path and "url=" in parsed_url.query:
                original_url_param = urllib.parse.parse_qs(parsed_url.query).get('url', [None])[0]
                if original_url_param:
                    original_path = urllib.parse.urlparse(original_url_param).path
                    original_base_name = os.path.basename(original_path)
                    if original_base_name and '.' in original_base_name:
                        return original_base_name
            if base_name and '.' not in base_name:
                return f"{base_name}{ext}"
            return f"image_p{page_number}_{int(time.time())}{ext}"
        return base_name
    except Exception as e:
        print(f"      Error generating filename for URL {url}: {e}")
        ext = ".jpg"
        return f"image_p{page_number}_fallback_{int(time.time())}{ext}"


def download_file_task(scraper_session, media_url, local_directory, target_filename, original_page_url,
                       page_number_for_log):
    """
    Task for downloading a single file (image).
    """
    if not media_url:
        print(f"  [P{page_number_for_log}] No valid URL for {target_filename}.")
        return False

    filepath = os.path.join(local_directory, target_filename)

    if os.path.exists(filepath):
        print(f"    [P{page_number_for_log}] Image {target_filename} already exists. Skipping.")
        return True

    if not os.path.exists(local_directory):
        try:
            os.makedirs(local_directory, exist_ok=True)
        except OSError as e:
            print(f"  [P{page_number_for_log}] Error creating directory {local_directory} for {target_filename}: {e}")
            return False

    download_headers = scraper_session.headers.copy()
    download_headers['Referer'] = original_page_url
    download_headers['Sec-Fetch-Dest'] = 'image'
    download_headers['Sec-Fetch-Site'] = 'same-origin'

    try:
        print(f"    [P{page_number_for_log}][Thread] Downloading Image: {media_url} to {target_filename}")
        with scraper_session.get(media_url, headers=download_headers, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"    [P{page_number_for_log}][Thread] Successfully downloaded Image: {target_filename}")
        if REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD > 0:
            time.sleep(REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD)
        return True
    except cloudscraper.exceptions.CloudflareChallengeError as e:
        print(
            f"    [P{page_number_for_log}][Thread] Cloudflare challenge downloading image {target_filename} from {media_url}: {e}")
    except requests.exceptions.Timeout:
        print(f"    [P{page_number_for_log}][Thread] Timeout downloading image {target_filename} from {media_url}")
    except requests.exceptions.RequestException as e:
        print(f"    [P{page_number_for_log}][Thread] Error downloading image {target_filename} from {media_url}: {e}")
    except IOError as e:
        print(f"    [P{page_number_for_log}][Thread] Error writing image file {filepath}: {e}")
    except Exception as e:
        print(f"    [P{page_number_for_log}][Thread] Unexpected error downloading image {target_filename}: {e}")
    return False


def scrape_page_for_image_task(page_number, scraper_instance, base_url_for_task, page_url_template_for_task):
    """
    Task for scraping a single page to find IMAGE URLs.
    Returns a list of image item dictionaries found on the page, or an empty list.
    """
    # Use the PAGE_URL_TEMPLATE read from environment or default
    page_url = page_url_template_for_task.format(page_number)
    current_referer = page_url_template_for_task.format(
        page_number - 1) if page_number > START_PAGE else base_url_for_task

    request_headers = scraper_instance.headers.copy()
    request_headers['Referer'] = current_referer

    print(f"  [P{page_number}][ScrapeThread] Scraping for IMAGE: {page_url}")
    image_items_on_page = []

    try:
        response = scraper_instance.get(page_url, headers=request_headers, timeout=45)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        image_url = None

        img_element = soup.select_one('img.h-auto.w-full.rounded-xl[src*="/_next/image?url="]')
        if not img_element: img_element = soup.select_one('img[data-nimg="1"][src*="/_next/image?url="]')
        if not img_element: img_element = soup.select_one('img[src*="/_next/image?url="]')

        if img_element and img_element.get('src'):
            img_src_attr = img_element['src']
            parsed_next_image_url = urllib.parse.urlparse(img_src_attr)
            query_params = urllib.parse.parse_qs(parsed_next_image_url.query)
            relative_img_path = query_params.get('url', [None])[0]

            if relative_img_path:
                image_url = urllib.parse.urljoin(base_url_for_task, relative_img_path.lstrip('/'))
                filename = generate_filename_from_url(image_url, page_number, "image")
                item_info = {
                    'media_url': image_url,
                    'filename': filename,
                    'original_page_url': page_url,
                    'page_number': page_number,
                    'type': "image"
                }
                image_items_on_page.append(item_info)
                print(f"    [P{page_number}][ScrapeThread] Found IMAGE: {image_url} (Save as: {filename})")
        else:
            print(f"    [P{page_number}][ScrapeThread] No target image found.")

    except cloudscraper.exceptions.CloudflareChallengeError as e:
        print(f"    [P{page_number}][ScrapeThread] Cloudflare challenge on page {page_url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"    [P{page_number}][ScrapeThread] Error fetching page {page_url}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                content_preview = e.response.content.decode('utf-8', errors='replace')[:200]
            except:
                content_preview = str(e.response.content[:200])
            print(f"      Response status: {e.response.status_code}. Preview: {content_preview}")
    except Exception as e:
        print(f"    [P{page_number}][ScrapeThread] Unexpected error scraping {page_url} for image: {e}")

    if REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD > 0:
        time.sleep(REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD)
    return image_items_on_page


# --- Main Script ---
def main():
    print(f"Initializing Image-Only Scraper...")
    print(f"Configuration loaded: ")
    print(f"  PAGE_URL_TEMPLATE: {PAGE_URL_TEMPLATE}")
    print(f"  START_PAGE: {START_PAGE}")
    print(f"  END_PAGE: {END_PAGE}")
    print(f"  DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    print(f"  MAX_CONCURRENT_SCRAPERS: {MAX_CONCURRENT_SCRAPERS}")
    print(f"  MAX_CONCURRENT_DOWNLOADERS: {MAX_CONCURRENT_DOWNLOADERS}")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        delay=10
    )
    scraper.headers.update(BASE_HEADERS)

    # Use the DOWNLOAD_DIR read from environment or default
    if not os.path.exists(DOWNLOAD_DIR):
        try:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            print(f"Created image download directory: {DOWNLOAD_DIR}")
        except OSError as e:
            print(f"Error creating image download directory {DOWNLOAD_DIR}: {e}. Exiting.")
            return

    collected_image_items = []

    print(
        f"\n--- Phase 1: Scraping Image URLs (Pages {START_PAGE} to {END_PAGE}) | {MAX_CONCURRENT_SCRAPERS} workers ---")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
        future_to_page = {
            # Pass the PAGE_URL_TEMPLATE from config to the task
            executor.submit(scrape_page_for_image_task, i, scraper, BASE_URL, PAGE_URL_TEMPLATE): i
            for i in range(START_PAGE, END_PAGE + 1)
        }

        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                page_image_items = future.result()
                if page_image_items:
                    collected_image_items.extend(page_image_items)
            except Exception as exc:
                print(f"  [Main][ScrapePhase] Page {page_num} (image scan) generated an exception in thread: {exc}")

    print(f"\n--- Phase 1 Finished: Collected {len(collected_image_items)} image items to download. ---")

    if not collected_image_items:
        print("No image items were found to download. Exiting.")
        return

    collected_image_items.sort(key=lambda x: x['page_number'])

    print(
        f"\n--- Phase 2: Downloading Images in batches of {DOWNLOAD_BATCH_SIZE} | {MAX_CONCURRENT_DOWNLOADERS} workers per batch ---")
    successful_downloads = 0
    failed_downloads = 0

    for i in range(0, len(collected_image_items), DOWNLOAD_BATCH_SIZE):
        current_batch_items = collected_image_items[i:i + DOWNLOAD_BATCH_SIZE]
        batch_number = (i // DOWNLOAD_BATCH_SIZE) + 1
        print(
            f"\n  Processing image download batch {batch_number} (Items {i + 1} to {min(i + DOWNLOAD_BATCH_SIZE, len(collected_image_items))})...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADERS) as executor:
            future_to_download = {
                executor.submit(
                    download_file_task,
                    scraper,
                    item['media_url'],
                    DOWNLOAD_DIR,  # Use DOWNLOAD_DIR from config
                    item['filename'],
                    item['original_page_url'],
                    item['page_number']
                ): item
                for item in current_batch_items
            }

            for future in concurrent.futures.as_completed(future_to_download):
                item_info = future_to_download[future]
                try:
                    success = future.result()
                    if success:
                        successful_downloads += 1
                    else:
                        failed_downloads += 1
                except Exception as exc:
                    print(
                        f"    [Main][DownloadBatch {batch_number}] Image download for {item_info.get('filename', 'Unknown file')} (Page {item_info.get('page_number')}) generated an exception: {exc}")
                    failed_downloads += 1

        print(f"  Finished image download batch {batch_number}.")
        if i + DOWNLOAD_BATCH_SIZE < len(collected_image_items):
            if DELAY_BETWEEN_DOWNLOAD_BATCHES > 0:
                print(f"  Pausing for {DELAY_BETWEEN_DOWNLOAD_BATCHES} seconds before next image batch...")
                time.sleep(DELAY_BETWEEN_DOWNLOAD_BATCHES)
            else:
                print(f"  Proceeding to next image batch immediately.")

    print("\n--- Image download process complete. ---")
    print(f"Successfully downloaded/skipped: {successful_downloads} images.")
    print(f"Failed image downloads: {failed_downloads} images.")


if __name__ == "__main__":
    main()
