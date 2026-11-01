import requests
import json
from datetime import date
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable


API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50


@task
def get_playlist_id():
    try:
        url = (
            "https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        )

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]
        channel_playlist_id = (
            channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        )

        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlist_id):
    video_ids = []
    page_token = None

    base_url = (
        "https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={maxResults}"
        f"&playlistId={playlist_id}&key={API_KEY}"
    )

    try:
        while True:
            url = base_url

            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            page_token = data.get("nextPageToken")

            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e


@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i:i + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)

            url = (
                "https://youtube.googleapis.com/youtube/v3/videos"
                "?part=contentDetails&part=snippet&part=statistics"
                f"&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item["statistics"]

                extracted_data.append(
                    {
                        "video_id": item["id"],
                        "title": snippet["title"],
                        "publishedAt": snippet["publishedAt"],
                        "duration": content_details["duration"],
                        "viewCount": statistics.get("viewCount"),
                        "likeCount": statistics.get("likeCount"),
                        "commentCount": statistics.get("commentCount"),
                    }
                )

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e


@task
def save_to_json(extracted_data):
    data_dir = Path("/opt/airflow/data")
    data_dir.mkdir(parents=True, exist_ok=True)

    file_path = data_dir / f"YT_data_{date.today()}.json"

    with file_path.open("w", encoding="utf-8") as json_outfile:
        json.dump(
            extracted_data,
            json_outfile,
            indent=4,
            ensure_ascii=False
        )