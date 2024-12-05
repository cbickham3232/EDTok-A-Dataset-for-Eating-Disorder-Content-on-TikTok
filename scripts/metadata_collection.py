"""
Description: 
    This script performs metadata video collection using the TikTok Research API.

Usage: 
    Requires a text file of the keywords and hashtags, contained in a specified directory.
    Requires the file path to your main CSV file or the file path of where you want the script to save the data.
    How to run the script: python3 metadata_collection.py

Dependencies:
    Required libraries or modules are listed in requirements.txt
"""
import sys
import requests
import json
from datetime import datetime, timedelta
import time
import csv
import pandas as pd
import os
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

# API keys (replace with your actual credentials or store securely)
client_key = "your_client_key"
client_secret = "your_client_secret"

def append_to_existing_or_create_new(df, combined_df_path):
    """
    Appends the data returned from the API to the main CSV file that contains everything collected thus far.

    Args: 
        df (Pandas DataFrame): DataFrame containing metadata of TikTok videos.
        combined_df_path (str): File path to the main CSV file.
    """
    # Load the existing combined DataFrame
    if os.path.exists(combined_df_path):
        combined_df = pd.read_csv(combined_df_path)
    else:
        combined_df = pd.DataFrame()

    # Iterate through each date in the new DataFrame
    for date in df['utc_date_string'].unique():
        df_date = df[df['utc_date_string'] == date]
        date_file_path = f"metadata_{date}.csv"

        if os.path.exists(date_file_path):
            existing_df = pd.read_csv(date_file_path)
            updated_df = pd.concat([existing_df, df_date], ignore_index=True).drop_duplicates(subset=['id'], keep='first')
            updated_df.to_csv(date_file_path, index=False)
        else:
            df_date.to_csv(date_file_path, index=False)

        combined_df = pd.concat([combined_df, df_date], ignore_index=True)

    combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
    print("Total entries in the combined CSV file:", len(combined_df))
    combined_df.to_csv(combined_df_path, index=False)

def save_to_json_file(data, filename):
    """
    Saves data to a JSON file.

    Args:
        data (dict): The response from the API and additional attributes.
        filename (str): Path to the JSON file.
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def createURL(username, videoid):
    """
    Creates the URL for a TikTok post.

    Args: 
        username (str): Username of the person who posted the TikTok video.
        videoid (int): ID of the video.

    Returns: 
        str: The URL to the TikTok post.
    """
    return f"https://www.tiktok.com/@{username}/video/{videoid}"

def convert_epoch_to_datetime(input_time):
    """
    Converts Epoch/Unix time to UTC time.

    Args:
        input_time (int): Time in Epoch/Unix format.

    Returns:
        pd.Series: Extracted date and time components.
    """
    utc_time_stamp = datetime.utcfromtimestamp(input_time)
    return pd.Series([
        utc_time_stamp.year, utc_time_stamp.month, utc_time_stamp.day,
        utc_time_stamp.hour, utc_time_stamp.minute, utc_time_stamp.second,
        utc_time_stamp.strftime("%Y-%m-%d"), utc_time_stamp.strftime("%H:%M:%S")
    ])

def get_access_token(client_key, client_secret):
    """
    Fetches the Research API access token.

    Args: 
        client_key (str): Client key from TikTok's developer portal.
        client_secret (str): Client secret from TikTok's developer portal.

    Returns: 
        dict: Dictionary containing access token information.
    """
    endpoint_url = "https://open.tiktokapis.com/v2/oauth/token/"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'client_key': client_key, 'client_secret': client_secret, 'grant_type': 'client_credentials'}
    response = requests.post(endpoint_url, headers=headers, data=data)

    if response.status_code == 200:
        response_json = response.json()
        return {key: response_json[key] for key in ['access_token', 'expires_in', 'token_type'] if key in response_json}
    else:
        print("Error fetching token:", response.json())
        return {}

# Replace sensitive paths and variables
start_date = "YYYYMMDD"
combined_file_path = "path_to_combined_file.csv"
keywords_file_path = "path_to_keywords_and_hashtags.txt"

# Read keywords and hashtags
with open(keywords_file_path, 'r') as file:
    lines = [line.strip() for line in file if line.strip()]

keywords_list = lines
hashtags_list = lines

# Fetch access token
token_info = get_access_token(client_key, client_secret)
token_info['expires_at'] = time.time() + token_info.get('expires_in', 0)

while start_date != "YYYYMMDD_END":
    end_date_obj = datetime.strptime(start_date, "%Y%m%d") + timedelta(days=1)
    end_date_str = end_date_obj.strftime("%Y%m%d")
    print("Start date:", start_date, "End date:", end_date_str)

    # Fetch data and save
    data, total_count = fetch_tiktok_data(start_date, end_date_str, keywords_list, hashtags_list, token_info)
    print("Total videos fetched:", total_count)

    if data:
        save_to_json_file(data, f"{start_date}_{end_date_str}_metadata.json")
        df = pd.DataFrame(data['data']['videos'])
        df['tiktokurl'] = df.apply(lambda row: createURL(row['username'], row['id']), axis=1)
        df[['utc_year', 'utc_month', 'utc_day', 'utc_hour', 'utc_minute', 'utc_second', 'utc_date_string', 'utc_time_string']] = df['create_time'].apply(convert_epoch_to_datetime)
        append_to_existing_or_create_new(df, combined_file_path)

    start_date = end_date_str
