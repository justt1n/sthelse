import cloudscraper
from bs4 import BeautifulSoup
import os
import urllib.parse
import time
import concurrent.futures
import requests  # For type hinting and specific exceptions
from dotenv import load_dotenv  # Import for .env file loading
import json  # For parsing JSON API responses
import yt_dlp  # Import yt-dlp
import uuid  # For generating unique fallback IDs
import shutil  # For checking ffmpeg path

# --- Load Environment Variables ---
load_dotenv()  # Load variables from .env file into environment

# --- Configuration ---
BASE_URL = os.getenv("BASE_URL", "https://18tube.org")
PROFILE_PAGE_URL = os.getenv("PROFILE_PAGE_URL")
API_MEDIA_ENDPOINT = os.getenv("API_MEDIA_ENDPOINT", "https://18tube.org/wp-json/myapi/v1/media-items")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "18tube_full_videos_debug")  # Updated dir name

MAX_CONCURRENT_DOWNLOADERS = int(os.getenv("MAX_CONCURRENT_DOWNLOADERS", "1"))  # Default to 1 for debugging
DOWNLOAD_BATCH_SIZE = int(os.getenv("DOWNLOAD_BATCH_SIZE", "5"))  # Smaller batch for debugging

REQUEST_DELAY_SECONDS_API_CALL = int(os.getenv("REQUEST_DELAY_SECONDS_API_CALL", "1"))
REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD = int(os.getenv("REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD", "0"))
DELAY_BETWEEN_DOWNLOAD_BATCHES = int(os.getenv("DELAY_BETWEEN_DOWNLOAD_BATCHES", "5"))
FFMPEG_LOCATION = os.getenv("FFMPEG_LOCATION", None)

BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'X-Requested-With': 'XMLHttpRequest',
}


# --- Helper Functions ---
def check_ffmpeg():
    """Checks if ffmpeg is accessible."""
    if FFMPEG_LOCATION and os.path.exists(FFMPEG_LOCATION):
        print(f"INFO: Using FFMPEG_LOCATION: {FFMPEG_LOCATION}")
        return True
    elif shutil.which("ffmpeg"):
        print("INFO: ffmpeg found in system PATH.")
        return True
    else:
        print("WARNING: ffmpeg not found in system PATH and FFMPEG_LOCATION is not set or invalid.")
        print("         Full video downloads (HLS merging) will likely fail.")
        print("         Please install ffmpeg and add it to PATH, or set FFMPEG_LOCATION in your .env file.")
        return False


def generate_video_filename(unique_id_for_file, original_m3u8_url):
    """Generates a unique video filename (e.g., unique_id.mp4 or unique_id_original_name.mp4)."""
    try:
        safe_unique_id = str(unique_id_for_file).replace('/', '_').replace('\\', '_').replace(':', '_').strip()

        path = urllib.parse.urlparse(original_m3u8_url).path
        original_basename_from_url = os.path.basename(path)
        name_part_from_url, _ = os.path.splitext(original_basename_from_url)

        output_ext = ".mp4"

        if name_part_from_url and name_part_from_url.lower() not in ["index", "playlist", ""]:
            filename = f"{safe_unique_id}_{name_part_from_url}{output_ext}"
        else:
            filename = f"{safe_unique_id}{output_ext}"
        return filename
    except Exception as e:
        print(f"      Error generating video filename for ID {unique_id_for_file}: {e}")
        safe_unique_id_fallback = str(unique_id_for_file).replace('/', '_').replace('\\', '_').replace(':', '_').strip()
        return f"video_{safe_unique_id_fallback}_fallback_{int(time.time())}.mp4"


def download_full_video_task(m3u8_url, local_directory, target_filename, original_page_url_for_referer,
                             item_id_for_log):
    """Task for downloading a full video using yt-dlp from an m3u8 URL."""
    if not m3u8_url:
        print(f"  [Item {item_id_for_log}] No valid m3u8 URL for {target_filename}.")
        return False

    if not os.path.exists(local_directory):
        try:
            os.makedirs(local_directory, exist_ok=True)
        except OSError as e:
            print(f"  [Item {item_id_for_log}] Error creating directory {local_directory} for {target_filename}: {e}")
            return False

    filepath = os.path.join(local_directory, target_filename)

    if os.path.exists(filepath):
        try:
            if os.path.getsize(
                    filepath) > 1024 * 10:  # Greater than 10KB, more likely to be a real (partial or full) video
                print(
                    f"    [Item {item_id_for_log}] Full Video {target_filename} already exists and is >10KB. Skipping.")
                return True
            else:
                print(
                    f"    [Item {item_id_for_log}] Full Video {target_filename} exists but is very small (<10KB). Attempting re-download.")
        except OSError:
            print(
                f"    [Item {item_id_for_log}] Full Video {target_filename} found, but couldn't get size. Will attempt download.")

    http_headers = {
        'Referer': original_page_url_for_referer if original_page_url_for_referer else BASE_URL,
        'User-Agent': BASE_HEADERS['User-Agent']
    }

    ydl_opts = {
        'outtmpl': filepath,
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        # 'noprogress': True, # Disabled for debugging
        # 'quiet': True, # Disabled for debugging
        'verbose': True,  # Enabled for debugging
        'no_warnings': False,  # Show warnings for debugging
        'http_headers': http_headers,
        'retries': 2,  # Reduced retries for faster debugging cycles
        'fragment_retries': 2,  # Reduced retries for faster debugging cycles
        'continuedl': True,
        'nopart': False,
        'no_mtime': True,  # Avoid issues with file modification times
        # 'writedescription': True, # Could be useful for metadata
        # 'writesubtitles': True,
        # 'writeautomaticsub': True,
    }
    if FFMPEG_LOCATION:
        ydl_opts['ffmpeg_location'] = FFMPEG_LOCATION

    try:
        print(
            f"    [Item {item_id_for_log}][Thread] Downloading Full Video: {m3u8_url} to {target_filename} with verbose yt-dlp output...")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([m3u8_url])
        # Check file size after download
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:  # Check if file is > 1KB
            print(
                f"    [Item {item_id_for_log}][Thread] Successfully downloaded Full Video: {target_filename} (Size: {os.path.getsize(filepath) / 1024:.2f} KB)")
        else:
            print(
                f"    [Item {item_id_for_log}][Thread] Download completed for {target_filename}, but file is very small or missing. Possible failure.")
            return False  # Consider it a failure if file is too small

        if REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD > 0:
            time.sleep(REQUEST_DELAY_SECONDS_DOWNLOAD_PER_THREAD)
        return True
    except yt_dlp.utils.DownloadError as e:
        print(f"    [Item {item_id_for_log}][Thread] yt-dlp DownloadError for {target_filename} from {m3u8_url}:")
        print(f"      {e}")  # Print the full yt-dlp error
    except Exception as e:
        print(
            f"    [Item {item_id_for_log}][Thread] Unexpected error downloading full video {target_filename} with yt-dlp: {e}")

    # Cleanup attempt for partial files on error
    # yt-dlp with 'nopart': False usually handles its .part files, but this is an extra check.
    # If the final file is tiny or non-existent, and a .part file for it exists, remove .part.
    part_filepath_pattern = filepath + ".part"  # yt-dlp might also use ffp.part for ffmpeg parts

    # Check for common part file extensions yt-dlp might use
    possible_part_files = [filepath + ext for ext in [".part", ".ytdl", ".ffp.part"]]

    for part_file in possible_part_files:
        if os.path.exists(part_file):
            try:
                os.remove(part_file)
                print(f"    [Item {item_id_for_log}][Thread] Removed partial/temp file: {part_file}")
            except OSError as e_rm:
                print(f"    [Item {item_id_for_log}][Thread] Error removing partial/temp file {part_file}: {e_rm}")

    # If the main file was created but is tiny, consider removing it too
    if os.path.exists(filepath) and os.path.getsize(filepath) < 1024:
        try:
            os.remove(filepath)
            print(f"    [Item {item_id_for_log}][Thread] Removed tiny/corrupt final file: {filepath}")
        except OSError as e_rm:
            print(f"    [Item {item_id_for_log}][Thread] Error removing tiny/corrupt final file {filepath}: {e_rm}")

    return False


def fetch_profile_data_id(scraper_instance, profile_url):
    print(f"  Fetching profile page to get data-id: {profile_url}")
    try:
        page_headers = BASE_HEADERS.copy()
        page_headers[
            'Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
        page_headers['Referer'] = BASE_URL

        response = scraper_instance.get(profile_url, headers=page_headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        tab_content_div = soup.select_one('div#tab-content.tab-content.onlyfans')
        if tab_content_div and tab_content_div.has_attr('data-id'):
            data_id = tab_content_div['data-id']
            print(f"    Successfully fetched data-id: {data_id}")
            return data_id
        else:
            print("    Could not find data-id on the profile page. HTML structure might have changed.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"    Error fetching profile page {profile_url}: {e}")
        return None
    except Exception as e:
        print(f"    Unexpected error fetching data-id: {e}")
        return None


# --- Main Script ---
def main():
    if not PROFILE_PAGE_URL:
        print("ERROR: PROFILE_PAGE_URL is not set in the .env file. Please define it.")
        return

    print(f"Initializing 18tube.org Full Video Scraper (yt-dlp Debugging Mode)...")
    print(f"Configuration loaded from .env (with defaults): ")
    print(f"  PROFILE_PAGE_URL: {PROFILE_PAGE_URL}")
    print(f"  DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    print(
        f"  MAX_CONCURRENT_DOWNLOADERS: {MAX_CONCURRENT_DOWNLOADERS} (Set to 1 in .env for initial debugging recommended)")

    if not check_ffmpeg():  # Check for ffmpeg at the start
        # Optionally, you could exit here if ffmpeg is critical and not found
        # return
        pass

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

    profile_data_id = fetch_profile_data_id(scraper, PROFILE_PAGE_URL)
    if not profile_data_id:
        print("Could not retrieve profile data-id. Exiting.")
        return

    collected_video_items = []
    api_page_num = 1
    ITEMS_PER_API_PAGE_EXPECTED = 24

    print(f"\n--- Phase 1: Scraping Video M3U8 URLs via API (Profile ID: {profile_data_id}) ---")

    while True:
        print(f"  Fetching API page {api_page_num} for videos...")
        api_params = {'page': api_page_num, 'type': 'videos', 'id': profile_data_id}
        api_request_headers = scraper.headers.copy()
        api_request_headers['Referer'] = PROFILE_PAGE_URL

        try:
            response = scraper.get(API_MEDIA_ENDPOINT, params=api_params, headers=api_request_headers, timeout=30)
            response.raise_for_status()
            content = response.json()
            media_data_list = content.get('data') if isinstance(content, dict) and 'data' in content else content

            if not isinstance(media_data_list, list) or not media_data_list:
                print(f"    No more video items found on API page {api_page_num} or unexpected format. End of content.")
                break

            found_on_this_page_count = 0
            for item in media_data_list:
                if isinstance(item, dict) and item.get('type') == 1 and item.get('source'):
                    m3u8_url = item['source']
                    item_api_id_from_json = item.get('id')

                    unique_identifier_for_file = None
                    if item_api_id_from_json is not None:
                        unique_identifier_for_file = str(item_api_id_from_json)
                    else:
                        path_segments = [seg for seg in urllib.parse.urlparse(m3u8_url).path.split('/') if
                                         seg.isdigit()]
                        if path_segments:
                            unique_identifier_for_file = f"pathid_{'_'.join(path_segments)}"
                        else:
                            unique_identifier_for_file = f"fallback_{uuid.uuid4().hex[:12]}"
                        print(
                            f"    Warning: Item for URL {m3u8_url} missing API 'id'. Using identifier: {unique_identifier_for_file}")

                    video_filename = generate_video_filename(unique_identifier_for_file, m3u8_url)

                    item_info = {
                        'm3u8_url': m3u8_url,
                        'video_filename': video_filename,
                        'original_page_url': PROFILE_PAGE_URL,
                        'item_api_id': unique_identifier_for_file,
                        'type': "video"
                    }
                    collected_video_items.append(item_info)
                    print(
                        f"    [API Page {api_page_num}] Found Video M3U8: {m3u8_url} (Will save as: {video_filename})")
                    found_on_this_page_count += 1

            if len(media_data_list) < ITEMS_PER_API_PAGE_EXPECTED:
                print(f"    API page {api_page_num} returned {len(media_data_list)} items. Assuming end of content.")
                break

            api_page_num += 1
            if REQUEST_DELAY_SECONDS_API_CALL > 0: time.sleep(REQUEST_DELAY_SECONDS_API_CALL)

        except requests.exceptions.RequestException as e:
            print(f"    Error fetching API page {api_page_num}: {e}")
            break
        except json.JSONDecodeError:
            print(
                f"    Error: API response for page {api_page_num} is not valid JSON. Response text: {response.text[:200]}")
            break
        except Exception as e:
            print(f"    Unexpected error processing API page {api_page_num}: {e}")
            break

    print(f"\n--- Phase 1 Finished: Collected {len(collected_video_items)} video M3U8 URLs. ---")

    if not collected_video_items:
        print("No video items were found to download. Exiting.")
        return

    print(
        f"\n--- Phase 2: Downloading Full Videos in batches of {DOWNLOAD_BATCH_SIZE} | {MAX_CONCURRENT_DOWNLOADERS} workers per batch ---")
    successful_downloads = 0
    failed_downloads = 0

    for i in range(0, len(collected_video_items), DOWNLOAD_BATCH_SIZE):
        current_batch_items = collected_video_items[i:i + DOWNLOAD_BATCH_SIZE]
        batch_number = (i // DOWNLOAD_BATCH_SIZE) + 1
        print(
            f"\n  Processing full video download batch {batch_number} (Items {i + 1} to {min(i + DOWNLOAD_BATCH_SIZE, len(collected_video_items))})...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADERS) as executor:
            future_to_download = {
                executor.submit(
                    download_full_video_task,
                    item['m3u8_url'],
                    DOWNLOAD_DIR,
                    item['video_filename'],
                    item['original_page_url'],
                    item['item_api_id']
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
                        f"    [Main][DownloadBatch {batch_number}] Full Video download for {item_info.get('video_filename', 'Unknown file')} (Item ID {item_info.get('item_api_id')}) generated an exception: {exc}")
                    failed_downloads += 1

        print(f"  Finished full video download batch {batch_number}.")
        if i + DOWNLOAD_BATCH_SIZE < len(collected_video_items):
            if DELAY_BETWEEN_DOWNLOAD_BATCHES > 0:
                print(f"  Pausing for {DELAY_BETWEEN_DOWNLOAD_BATCHES} seconds before next batch...")
                time.sleep(DELAY_BETWEEN_DOWNLOAD_BATCHES)
            else:
                print(f"  Proceeding to next batch immediately.")

    print("\n--- Full Video download process complete. ---")
    print(f"Successfully downloaded/skipped: {successful_downloads} full videos.")
    print(f"Failed full video downloads: {failed_downloads} full videos.")


if __name__ == "__main__":
    main()
