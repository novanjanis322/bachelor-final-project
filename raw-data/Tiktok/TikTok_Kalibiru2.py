from TikTokApi import TikTokApi
import asyncio
import os
import csv
from datetime import datetime


video_id=7388580503537896710 
#https://vt.tiktok.com/ZSYXJwd1F/ bukit bintang

ms_token = os.environ.get("uFga4LlA9YKs0XcTPMYzwafw5Z_TKewN49h8t2pRDyZ4chxF_Qy6Kgh5LA9ZwYos9BQd7wPHTtkHYUk3QVD50eHHoZwnqrUla5z9pAuuoYdsoBYmR7vwsT3nu8vGuRoxPxTMe4BfR4fw8fXDiP7z-A==", None) # get your own ms_token from your cookies on tiktok.com

async def trending_videos():
    async with TikTokApi() as api:
        await api.create_sessions(headless=False, ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
        async for video in api.trending.videos(count=5):
            print(video)
            video = str(video) + "\n"
            with open('test3.json', 'a') as file:
                file.write(video)

async def get_comments():
    async with TikTokApi() as api:
        await api.create_sessions(headless=False, ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
        video = api.video(id=video_id)
        comments = []
        async for comment in video.comments(count=5000):
            comment_dict = comment.as_dict
            # Extract the create_time field from the dictionary
            create_time = comment_dict.get('create_time')
            comment_date = datetime.fromtimestamp(create_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            a=[comment.id, comment.author.user_id, comment.author.username, comment.text, comment.likes_count, comment_date]
            comments.append(a)
        # print(comments)
    file_name = os.path.splitext(os.path.basename(__file__))[0]
    csv_file_path = f"{file_name}.csv"
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ['Comment_ID', 'Author_ID','Author','Text', 'Likes','Comment_Date']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()  
        sorted_comments = sorted(comments, key=lambda x: x[4], reverse=True)
        for comment in sorted_comments:
            writer.writerow({'Comment_ID': comment[0],'Author_ID':comment[1], 'Author': comment[2], 'Text': comment[3], 'Likes': comment[4], 'Comment_Date': comment[5]})
    print("Comments saved to", csv_file_path)


if __name__ == "__main__":
    asyncio.run(get_comments())


# async def get_comments():
#     async with TikTokApi() as api:
#         await api.create_sessions(headless=False, ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
#         video = api.video(id=video_id)
#         comments = []
#         async for comment in video.comments(count=5000):
#             a=[comment.id, comment.author.user_id, comment.author.username, comment.text, comment.likes_count]
#             comments.append(a)
#         # print(comments)
#     file_name = os.path.splitext(os.path.basename(__file__))[0]
#     csv_file_path = f"{file_name}.csv"
#     with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
#         fieldnames = ['Comment_ID', 'Author_ID','Author','Text', 'Likes']
#         writer = csv.DictWriter(file, fieldnames=fieldnames)
#         writer.writeheader()  
#         for comment in comments:
#             writer.writerow({'Comment_ID': comment[0],'Author_ID':comment[1], 'Author': comment[2], 'Text': comment[3], 'Likes': comment[4]})
#     print("Comments saved to", csv_file_path)

# comment_dict = comment.as_dict
            # # Print out the keys and values of the comment dictionary
            # for key, value in comment_dict.items():
            #     print(f"{key}: {value}")
            # break

            
            # # print(dir(comment.as_dict))