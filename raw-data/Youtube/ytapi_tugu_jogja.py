import googleapiclient.discovery
import pandas as pd
import os

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = os.getenv('DEVELOPER_KEY')
video_id = "0VxLHWzb4Ac"

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)

next_page_token = None
comments = []
while True:
    response = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100, 
        pageToken=next_page_token
    ).execute()


    comments.extend(response['items'])


    next_page_token = response.get('nextPageToken')
    if not next_page_token:
        break

comment_data = []

for item in comments:
    comment_id = item['snippet']['topLevelComment']['id']
    text = item['snippet']['topLevelComment']['snippet']['textOriginal']
    author_channel_id = item['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
    author_display_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
    like_count = item['snippet']['topLevelComment']['snippet']['likeCount']
    published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']
    
    comment_data.append({
        'id': comment_id,
        'textOriginal': text,
        'authorChannelId': author_channel_id,
        'authorDisplayName': author_display_name,
        'likeCount': like_count,
        'publishedAt': published_at
    })


    reply_response = youtube.comments().list(
        part='snippet',
        parentId=item['snippet']['topLevelComment']['id'],
        maxResults=100
    ).execute()


    for reply in reply_response['items']:
        reply_id = reply['id'].split('.')[1]
        reply_text = reply['snippet']['textOriginal']
        reply_author_channel_id = reply['snippet']['authorChannelId']['value']
        reply_author_display_name = reply['snippet']['authorDisplayName']
        reply_like_count = reply['snippet']['likeCount']
        reply_published_at = reply['snippet']['publishedAt']
        
        comment_data.append({
            'id': reply_id,
            'textOriginal': reply_text,
            'authorChannelId': reply_author_channel_id,
            'authorDisplayName': reply_author_display_name,
            'likeCount': reply_like_count,
            'publishedAt': reply_published_at
        })

df = pd.DataFrame(comment_data)

file_name = os.path.splitext(os.path.basename(__file__))[0]
df_sorted = df.sort_values(by='likeCount', ascending=False)
df_sorted.to_csv(f'{file_name}.csv', index=False)
print(df)
print(df.columns)
is_duplicate = df['id'].duplicated().any()
if is_duplicate:
    print("There are duplicate values in the column.")
else:
    print("There are no duplicate values in the column.")
