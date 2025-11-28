import requests
import json 
#from dotenv import load_dotenv
#import os
from datetime import date
from airflow.decorators import dag, task
from airflow.models import Variable
#load_dotenv(dotenv_path='.env') 

API_KEY = Variable.get("API_KEY")
CHANEL_HANDEL = Variable.get('CHANNEL_HANDLE')
maxResults= 50


@task
def get_uploads_playlist_id():
    
    try:


        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANEL_HANDEL}&key={API_KEY}"

        response = requests.get(url) 
        response.raise_for_status()


        data= response.json() 
        

        chanel_id = data['items'][0] 
        uploads_id = chanel_id['contentDetails']['relatedPlaylists']['uploads']
        #print("Uploads Playlist ID:", uploads_id) # -> for testing purpose
        
        return uploads_id
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_id(playlist_id):
    video_ids = []
    page_token = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}"
    try: 
        
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            page_token = data.get('nextPageToken')
            if not page_token:
                break
            return video_ids
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(vide_ids):
    
    extracted_data = []
    url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id=lQz5aqjZ3lQ&key={API_KEY}"


    def batch_list(video_ids, batch_size):
    
        for video_id in range(0, len(video_ids), batch_size):
            yield video_ids[video_id: video_id + batch_size]
    
    try:
        for batch in batch_list(vide_ids,maxResults):
            video_id_str = ','.join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_id_str}&key={API_KEY}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items', []):
                
                video_id = item['id']
                sinnpet = item['snippet']
                statistics = item['statistics']
                content_details = item['contentDetails']
                
                video_data = {
                    'video_id': video_id,
                    'publishedAt':sinnpet['publishedAt'],
                    'title': sinnpet['title'],
                    'duration': content_details['duration'],
                    'viewCount': statistics.get('viewCount', None),
                    'likeCount': statistics.get('likeCount', None),
                    'commentCount': statistics.get('commentCount', None)
                }
            
                extracted_data.append(video_data)
        return extracted_data   
                    
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def save_to_json(extrcated_data):
    path = f"./data/video_data_{date.today()}.json" 
    try:
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(extrcated_data, file, ensure_ascii=False, indent=4)

    except IOError as e:
        raise e

if __name__ == "__main__":
    playlist_id = get_uploads_playlist_id()
    video_ids= get_video_id(playlist_id)
    video_data_yt= extract_video_data(video_ids)
    save_to_json(video_data_yt)
