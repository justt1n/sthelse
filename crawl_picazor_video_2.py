import cloudscraper
from bs4 import BeautifulSoup
import os
import urllib.parse
import time
import concurrent.futures
import requests  # For type hinting and specific exceptions

# --- Configuration ---
BASE_URL = "https://picazor.com"
PAGE_URL_TEMPLATE = "https://picazor.com/en/thanh-nhen/{}"
START_PAGE = 1
END_PAGE = 568  # Inclusive
DOWNLOAD_DIR = "picazor_thanh_nhen_videos_only_v1"  # Directory for videos

# Concurrency Settings
MAX_CONCURRENT_SCRAPERS = 30  # Number of concurrent threads for scraping video URLs (Phase 1)
MAX_CONCURRENT_DOWNLOADERS = 5  # Number of concurrent threads for downloading videos (Phase 2, per batch)
DOWNLOAD_BATCH_SIZE = 100  # Number of videos to download in each batch

# Delay Settings
REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD = 1  # Delay after each thread's scrape attempt
REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD = 1  # Delay after each thread's download attempt
DELAY_BETWEEN_DOWNLOAD_BATCHES = 5  # Optional delay in seconds between download batches

# Base headers for the scraper
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    # 'Referer' will be set per request
}


# --- Helper Functions ---
def generate_filename_from_url(url, page_number, media_type_hint="video"):  # Default to video
    """Generates a filename from a URL, page number, and media type hint."""
    try:
        path = urllib.parse.urlparse(url).path
        base_name = os.path.basename(path)
        if not base_name or '.' not in base_name:  # No filename or no extension
            ext = ".mp4"  # Assume mp4 for videos if not determinable
            if base_name and '.' not in base_name:
                return f"{base_name}{ext}"
            return f"video_p{page_number}_{int(time.time())}{ext}"
        return base_name
    except Exception as e:
        print(f"      Error generating filename for URL {url}: {e}")
        ext = ".mp4"
        return f"video_p{page_number}_fallback_{int(time.time())}{ext}"


def download_file_task(scraper_session, media_url, local_directory, target_filename, original_page_url,
                       page_number_for_log):
    """
    Task for downloading a single file (video).
    """
    if not media_url:
        print(f"  [P{page_number_for_log}] No valid URL for {target_filename}.")
        return False

    filepath = os.path.join(local_directory, target_filename)

    if os.path.exists(filepath):
        print(f"    [P{page_number_for_log}] Video {target_filename} already exists. Skipping.")
        return True

    if not os.path.exists(local_directory):
        try:
            os.makedirs(local_directory, exist_ok=True)
        except OSError as e:
            print(f"  [P{page_number_for_log}] Error creating directory {local_directory} for {target_filename}: {e}")
            return False

    download_headers = scraper_session.headers.copy()
    download_headers['Referer'] = original_page_url
    download_headers['Sec-Fetch-Dest'] = 'video'  # Explicitly video
    download_headers['Sec-Fetch-Site'] = 'same-origin'  # Adjust if media is on CDN

    try:
        print(f"    [P{page_number_for_log}][Thread] Downloading Video: {media_url} to {target_filename}")
        with scraper_session.get(media_url, headers=download_headers, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"    [P{page_number_for_log}][Thread] Successfully downloaded Video: {target_filename}")
        if REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD > 0:
            time.sleep(REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD)
        return True
    except cloudscraper.exceptions.CloudflareChallengeError as e:
        print(
            f"    [P{page_number_for_log}][Thread] Cloudflare challenge downloading video {target_filename} from {media_url}: {e}")
    except requests.exceptions.Timeout:
        print(f"    [P{page_number_for_log}][Thread] Timeout downloading video {target_filename} from {media_url}")
    except requests.exceptions.RequestException as e:
        print(f"    [P{page_number_for_log}][Thread] Error downloading video {target_filename} from {media_url}: {e}")
    except IOError as e:
        print(f"    [P{page_number_for_log}][Thread] Error writing video file {filepath}: {e}")
    except Exception as e:
        print(f"    [P{page_number_for_log}][Thread] Unexpected error downloading video {target_filename}: {e}")
    return False


def scrape_page_for_video_task(page_number, scraper_instance, base_url_for_task, page_url_template_for_task):
    """
    Task for scraping a single page to find VIDEO URLs.
    Returns a list of video item dictionaries found on the page, or an empty list.
    """
    page_url = page_url_template_for_task.format(page_number)
    current_referer = page_url_template_for_task.format(
        page_number - 1) if page_number > START_PAGE else base_url_for_task

    request_headers = scraper_instance.headers.copy()
    request_headers['Referer'] = current_referer

    print(f"  [P{page_number}][ScrapeThread] Scraping for VIDEO: {page_url}")
    video_items_on_page = []

    try:
        response = scraper_instance.get(page_url, headers=request_headers, timeout=45)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        video_url = None

        # Look for video
        # XPath: /html/body/div[5]/main/div[2]/div/div[1]/div/div[2]/div/figure/video
        # CSS selector for video source: video > source
        video_source_element = soup.select_one(
            'div.flex.flex-col.items-center.justify-center > figure > video > source[src]')
        if not video_source_element: video_source_element = soup.select_one('video > source[src^="/uploads/"]')
        if not video_source_element: video_source_element = soup.select_one('video > source[src*=".mp4"]')
        if not video_source_element: video_source_element = soup.select_one('video > source[src]')  # Most general

        if video_source_element and video_source_element.get('src'):
            relative_video_path = video_source_element['src']
            video_url = urllib.parse.urljoin(base_url_for_task, relative_video_path.lstrip('/'))

            filename = generate_filename_from_url(video_url, page_number, "video")
            item_info = {
                'media_url': video_url,
                'filename': filename,
                'original_page_url': page_url,
                'page_number': page_number,
                'type': "video"
            }
            video_items_on_page.append(item_info)
            print(f"    [P{page_number}][ScrapeThread] Found VIDEO: {video_url} (Save as: {filename})")
        else:
            print(f"    [P{page_number}][ScrapeThread] No target video found.")

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
        print(f"    [P{page_number}][ScrapeThread] Unexpected error scraping {page_url} for video: {e}")

    if REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD > 0:
        time.sleep(REQUEST_DELAY_SECONDS_SCRAPE_PER_THREAD)
    return video_items_on_page


# --- Main Script ---
def main():
    print(f"Initializing Video-Only Scraper...")
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
        delay=10
    )
    scraper.headers.update(BASE_HEADERS)

    if not os.path.exists(DOWNLOAD_DIR):
        try:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            print(f"Created video download directory: {DOWNLOAD_DIR}")
        except OSError as e:
            print(f"Error creating video download directory {DOWNLOAD_DIR}: {e}. Exiting.")
            return

    collected_video_items = []

    # --- Phase 1: Scrape all video URLs concurrently ---
    print(
        f"\n--- Phase 1: Scraping Video URLs (Pages {START_PAGE} to {END_PAGE}) | {MAX_CONCURRENT_SCRAPERS} workers ---")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
        future_to_page = {
            executor.submit(scrape_page_for_video_task, i, scraper, BASE_URL, PAGE_URL_TEMPLATE): i
            for i in range(START_PAGE, END_PAGE + 1)
        }

        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                page_video_items = future.result()
                if page_video_items:
                    collected_video_items.extend(page_video_items)
            except Exception as exc:
                print(f"  [Main][ScrapePhase] Page {page_num} (video scan) generated an exception in thread: {exc}")

    print(f"\n--- Phase 1 Finished: Collected {len(collected_video_items)} video items to download. ---")

    if not collected_video_items:
        print("No video items were found to download. Exiting.")
        return

    collected_video_items.sort(key=lambda x: x['page_number'])

    # --- Phase 2: Download all collected videos concurrently in batches ---
    print(
        f"\n--- Phase 2: Downloading Videos in batches of {DOWNLOAD_BATCH_SIZE} | {MAX_CONCURRENT_DOWNLOADERS} workers per batch ---")
    successful_downloads = 0
    failed_downloads = 0

    for i in range(0, len(collected_video_items), DOWNLOAD_BATCH_SIZE):
        current_batch_items = collected_video_items[i:i + DOWNLOAD_BATCH_SIZE]
        batch_number = (i // DOWNLOAD_BATCH_SIZE) + 1
        print(
            f"\n  Processing video download batch {batch_number} (Items {i + 1} to {min(i + DOWNLOAD_BATCH_SIZE, len(collected_video_items))})...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADERS) as executor:
            future_to_download = {
                executor.submit(
                    download_file_task,
                    scraper,
                    item['media_url'],
                    DOWNLOAD_DIR,
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
                        f"    [Main][DownloadBatch {batch_number}] Video download for {item_info.get('filename', 'Unknown file')} (Page {item_info.get('page_number')}) generated an exception: {exc}")
                    failed_downloads += 1

        print(f"  Finished video download batch {batch_number}.")
        if i + DOWNLOAD_BATCH_SIZE < len(collected_video_items):
            if DELAY_BETWEEN_DOWNLOAD_BATCHES > 0:
                print(f"  Pausing for {DELAY_BETWEEN_DOWNLOAD_BATCHES} seconds before next video batch...")
                time.sleep(DELAY_BETWEEN_DOWNLOAD_BATCHES)
            else:
                print(f"  Proceeding to next video batch immediately.")

    print("\n--- Video download process complete. ---")
    print(f"Successfully downloaded/skipped: {successful_downloads} videos.")
    print(f"Failed video downloads: {failed_downloads} videos.")


if __name__ == "__main__":
    main()
