'''
Download TikTok videos
'''
import pandas as pd
import pyktok as pyk
import time
import requests
from requests.exceptions import ReadTimeout
import os
from pathlib import Path
import logging
import sys

def setup_logging(file_path):
    """
    Sets up logging for the script.

    Args:
        file_path (Path): Path of the CSV file being processed.
    """
    file_name = file_path.stem
    log_file_name = f"{file_name}_process_log.log"

    logging.basicConfig(
        filename=log_file_name,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def isPrivate(row):
    """
    Checks if a TikTok video is publicly available.

    Args:
        row (pd.Series): Row of the DataFrame containing video metadata.

    Returns:
        bool: True if public, False if private.
    """
    username = row['username']
    video_id = row['id']
    max_attempts = 10

    for attempt in range(max_attempts):
        try:
            tt_json = pyk.alt_get_tiktok_json(f"https://www.tiktok.com/@{username}/video/{video_id}?is_copy_url=1&is_from_webapp=v1")
            private_status = tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']['privateItem']
            return not private_status
        except Exception as e:
            if isinstance(e, ReadTimeout):
                if attempt < max_attempts - 1:
                    time.sleep(100)
                else:
                    return False
            elif "webapp.video-detail" in str(e):
                return False
            else:
                if attempt < max_attempts - 1:
                    time.sleep(100)
                else:
                    return False

def is_mp4_file(file_path):
    """
    Validates if the downloaded file is a valid MP4.

    Args:
        file_path (str): Path to the file.

    Returns:
        bool: True if valid MP4, False otherwise.
    """
    try:
        with open(file_path, 'rb') as file:
            header = file.read(12)
            return header[4:8] == b'ftyp'
    except IOError:
        return False

def format_url(url):
    """
    Formats a TikTok video URL.

    Args:
        url (str): URL of the video.

    Returns:
        str: Formatted URL.
    """
    return url + '?is_copy_url=1&is_from_webapp=v1'

def save_video(url):
    """
    Downloads a TikTok video.

    Args:
        url (str): Formatted TikTok video URL.
    """
    pyk.save_tiktok(url, save_video=True, browser_name="chrome")

def download(row, video_folder_path):
    """
    Downloads a video and validates the file.

    Args:
        row (pd.Series): Row of the DataFrame containing video metadata.
        video_folder_path (str): Path to the folder for storing downloaded videos.

    Returns:
        bool: True if the MP4 is valid, False otherwise.
    """
    url = row['tiktokurl']
    max_attempts = 5

    for attempt in range(max_attempts):
        try:
            time.sleep(10)
            formatted_url = format_url(url)
            save_video(formatted_url)
            video_file_path = os.path.join(video_folder_path, f"@{row['username']}_video_{row['id']}.mp4")
            return is_mp4_file(video_file_path)
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(100)
            else:
                logging.error(f"Failed to download video: {url}")
                return False

def process_csv_file(file_path, output_dir, video_dir):
    """
    Processes a CSV file to download videos and save metadata.

    Args:
        file_path (Path): Path to the CSV file.
        output_dir (str): Directory to save processed metadata.
        video_dir (str): Directory to save downloaded videos.
    """
    start_time = time.time()
    df = pd.read_csv(file_path)

    os.makedirs(output_dir, exist_ok=True)
    video_folder_path = os.path.join(video_dir, file_path.stem)
    os.makedirs(video_folder_path, exist_ok=True)

    df['isPublic'] = df.apply(isPrivate, axis=1)
    df['mp4_isValid'] = df.apply(download, axis=1, args=(video_folder_path,))

    output_file = os.path.join(output_dir, f"{file_path.stem}_processed.csv")
    df.to_csv(output_file, index=False)

    execution_time = time.time() - start_time
    logging.info(f"Processed {file_path} in {execution_time:.2f} seconds")

if __name__ == "__main__":
    pyk.specify_browser('chrome')

    if len(sys.argv) != 4:
        print("Usage: python download_videos.py <path_to_csv_file> <output_directory> <video_directory>")
        sys.exit(1)

    csv_file_path = Path(sys.argv[1])
    output_directory = sys.argv[2]
    video_directory = sys.argv[3]

    setup_logging(csv_file_path)
    process_csv_file(csv_file_path, output_directory, video_directory)
