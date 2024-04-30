import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime
import re
from mysql.connector import IntegrityError
from googleapiclient.errors import HttpError
import time


#creating connection
connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Taffy&0402",
        database="youtube"
    )
cursor=connection.cursor()


#connecting with the youtube api
Api_key = "AIzaSyDZKY_9aoN0MPmi4hh2_rkgxwhjt2TE2AI"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=Api_key)




#getting channel detail using channel_id
def get_channel_videos(channel_id):
    try:
        cursor.execute("SELECT COUNT(*) FROM Channel WHERE channel_id = %s", (channel_id,))
        count = cursor.fetchone()[0]
        if count > 0:
            cursor.execute("SELECT * FROM Channel WHERE channel_id = %s", (channel_id,))
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=['channel_id', 'channel_name', 'channel_type', 'channel_views', 'channel_description'])
            return df
        else:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id,            
                maxResults=100
            )
            response = request.execute()   
            channel_data=[]
            if 'items' in response:
                for item in response['items']:
                    data = {
                        "channel_id": item["id"],
                        "channel_name": item["snippet"]["title"],
                        "channel_type": item["kind"],
                        "channel_views": item["statistics"]["viewCount"],
                        "channel_description": item["snippet"]["description"]
                    }
                    channel_data.append(data)

            df = pd.DataFrame(channel_data)
            for i, row in df.iterrows():
                try:
                    cursor.execute("INSERT INTO Channel (channel_id, channel_name, channel_type, channel_views, channel_description) SELECT %s, %s, %s, %s, %s FROM DUAL WHERE NOT EXISTS (SELECT * FROM Channel WHERE channel_id = %s)",
                                   (row['channel_id'], row['channel_name'], row['channel_type'], row['channel_views'], row['channel_description'], row['channel_id']))
                    connection.commit()
                except IntegrityError:
                    # Handle the case when the entry already exists
                    pass

            return df
    except IntegrityError as e:
        # Handle the IntegrityError
        st.error(f"IntegrityError: {e}")
        return None
    
    
    
#getting playlist details using channel_id
def get_channel_playlists(channel_id):
    if not channel_id:
        return None
    cursor.execute("SELECT COUNT(*) FROM playlist WHERE channel_id = %s", (channel_id,))
    count = cursor.fetchone()[0]
    if count > 0:
        cursor.execute("SELECT * FROM playlist WHERE channel_id = %s", (channel_id,))
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=['playlist_id', 'channel_id', 'playlist_name'])
        return df
    else:
        playlist_response = youtube.playlists().list(
            part='snippet',
            channelId=channel_id,
            maxResults=50,  # You can adjust this number according to your needs
        ).execute()
            
        playlists = []
        for item in playlist_response['items']:
            # Extract playlist details
            playlist_id = item['id']
            playlist_name = item['snippet']['title']
            
            # Add playlist details to the list
            playlists.append({
                'playlist_id': playlist_id,
                'channel_id': channel_id,
                'playlist_name': playlist_name
            })
            
        playlist_df=pd.DataFrame(playlists)
        cursor.execute("CREATE TABLE IF NOT EXISTS playlist (playlist_id VARCHAR(255), channel_id VARCHAR(255),playlist_name VARCHAR(255), UNIQUE (playlist_id, channel_id) )")

            # Insert DataFrame data into the table
        for i, row in playlist_df.iterrows():
            try:
                cursor.execute("INSERT INTO playlist (playlist_id, channel_id, playlist_name ) VALUES (%s, %s, %s)",
                                (row['playlist_id'], row['channel_id'], row['playlist_name']))
                connection.commit()  # Commit after each row insertion
            except IntegrityError:
                # Handle the case when the entry already exists
                pass
        return playlist_df




#used to convert the datetime format        
def iso8601_to_seconds(duration):
    duration_regex = re.compile(
        r'PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?'
    )
    match = duration_regex.match(duration)
    if match:
        hours = int(match.group('hours')) if match.group('hours') else 0
        minutes = int(match.group('minutes')) if match.group('minutes') else 0
        seconds = int(match.group('seconds')) if match.group('seconds') else 0
        return hours * 3600 + minutes * 60 + seconds
    else:
        return 0
    
    
    
#getting video ids from the given playlist
def get_video_ids(youtube, playlist_id):
    if not playlist_id:
        return [], None  # Return an empty list and None for playlist_id if playlist_id is empty

    video_ids = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids, playlist_id




#getting all the video details from the all the video_ids
def get_video_details_whole(youtube, video_ids, playlist_id):
    all_video_info = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            video_info['playlist_id'] = playlist_id  # Add playlist_id to the video_info dictionary
            
            # Fetch channel_id based on channelTitle
            cursor.execute("SELECT channel_id FROM channel WHERE channel_name = %s", (video_info['channelTitle'],))
            channel_id_result = cursor.fetchone()
            if channel_id_result:
                video_info['channel_id'] = channel_id_result[0]
            else:
                # If channel_id not found, set it to None or handle as per your requirement
                video_info['channel_id'] = None

            all_video_info.append(video_info)
    
    video_df_whole = pd.DataFrame(all_video_info)
    
    # Ensure playlist_id is included in the DataFrame
    if 'playlist_id' not in video_df_whole.columns:
        video_df_whole['playlist_id'] = playlist_id
    
    # Insert data into the database
    for i, row in video_df_whole.iterrows():
        cursor.execute("SELECT COUNT(*) FROM video WHERE video_id = %s", (row['video_id'],))
        count = cursor.fetchone()[0]
        if count == 0:  # Insert only if the video_id doesn't exist
            published_date = datetime.fromisoformat(row['publishedAt'].replace('Z', '+00:00')).strftime(
                '%Y-%m-%d %H:%M:%S')
            duration_seconds = iso8601_to_seconds(row['duration'])
            cursor.execute("INSERT INTO video (video_id, channel_title, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status, channel_id,playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)",
                (row['video_id'], row['channelTitle'], row['title'], row['description'], published_date,
                 row['viewCount'], row['likeCount'], row.get('favoriteCount', None), row['commentCount'], duration_seconds,
                 row['definition'], row['caption'],row['channel_id'], row['playlist_id']))
            connection.commit()  # Commit after each row insertion

    return video_df_whole







#gettinf video details from the video_id
def get_video_details(youtube, video_id, playlist_id):
    all_video_info = []

    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    for video in response['items']:
        stats_to_keep = {
            'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
            'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
            'contentDetails': ['duration', 'definition', 'caption']
        }
        video_info = {}
        video_info['video_id'] = video['id']

        for k in stats_to_keep.keys():
            for v in stats_to_keep[k]:
                try:
                    video_info[v] = video[k][v]
                except:
                    video_info[v] = None

        all_video_info.append(video_info)

    video_df = pd.DataFrame(all_video_info)

    for i, row in video_df.iterrows():
        cursor.execute("SELECT COUNT(*) FROM video WHERE video_id = %s", (row['video_id'],))
        count = cursor.fetchone()[0]
        if count == 0:  # Insert only if the video_id doesn't exist
            published_date = datetime.fromisoformat(row['publishedAt'].replace('Z', '+00:00')).strftime(
                '%Y-%m-%d %H:%M:%S')
            duration_seconds = iso8601_to_seconds(row['duration'])
            cursor.execute(
                "INSERT INTO video (video_id, channel_title, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status, channel_id,playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)",
                (row['video_id'], row['channelTitle'], row['title'], row['description'], published_date,
                 row['viewCount'], row['likeCount'], row['favoriteCount'], row['commentCount'], duration_seconds,
                 row['definition'], row['caption'],row['playlist_id']))
            connection.commit()  # Commit after each row insertion

    return video_df



#getting comment details from the video using id
def get_video_comments(video_id):
    # Check if video_id is empty
    if not video_id:
        return pd.DataFrame([])

    # Call the API to get comment threads
    try:
        comment_response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,  # You can adjust this number according to your needs
            order='time',    # Order by time of comment publication
        ).execute()
    except HttpError as e:
        st.error(f"Error fetching comments: {e}")
        return pd.DataFrame([])

    comments = []

    # Iterate over each comment thread
    for item in comment_response.get('items', []):
        # Extract comment details
        comment_id = item['id']
        comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
        comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
        comment_published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']

        # Add comment details to the list
        comments.append({
            'comment_id': comment_id,
            'comment_text': comment_text,
            'comment_author': comment_author,
            'comment_published_at': comment_published_at
        })

    comment_df = pd.DataFrame(comments)

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS comments (comment_id VARCHAR(255), video_id VARCHAR(255), comment_text TEXT, comment_author VARCHAR(255), comment_published_date DATETIME, UNIQUE (comment_id, video_id))")
    except IntegrityError as e:
        # Handle the case when the table already exists
        pass

    # Insert DataFrame data into the table, skipping duplicates
    for i, row in comment_df.iterrows():
        published_date = datetime.fromisoformat(row['comment_published_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        try:
            cursor.execute("INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published_date ) VALUES (%s, %s, %s, %s, %s)",
                           (row['comment_id'], video_id,row['comment_text'], row['comment_author'],  published_date))
            connection.commit()  # Commit after each row insertion
        except IntegrityError as e:
            # Handle the case when there's a duplicate entry
            pass

    return comment_df


       



#UI for the youtube scrapping project
st.sidebar.title('Menus')
selected_tab = st.sidebar.radio('', ['Data Fetching', 'Questions','View Data'])

#data fetching
if selected_tab == 'Data Fetching':
    st.title('Youtube Scraping ')
    container = st.container(border=True)
    channel_ids = container.text_area('Enter Channel IDs (seperated by comma)', height=200).strip().split(',')
    if container.button('Get Data'):
        for channel_id in channel_ids:
            channel_df = get_channel_videos(channel_id)
            playlist_df = get_channel_playlists(channel_id)
            if channel_df is not None:
                st.header("Channel Data :")
                st.write(channel_df)
                st.success("Channel data fetched successfully!")
                st.toast('Channel Data fetched')
                time.sleep(.5)
            if playlist_df is not None:
                st.header("Playlist data :")
                st.write(playlist_df)
                st.success("Playlist data fetched successfully!")
                st.toast('Playlist Data fetched')
                time.sleep(.5)

        st.success("Data fetched successfully!")
    
    
    container2 = st.container(border=True)
    playlist_id = container2.text_input("Playlist id")
    video_ids, playlist_id = get_video_ids(youtube, playlist_id)
    video_df_whole=get_video_details_whole(youtube , video_ids ,playlist_id)
    if container2.button("Get", key="second"):
        if video_df_whole is not None:
            st.header("Video Ids :")
            st.write(video_df_whole)
            st.success("Videos ids are given")
            st.toast('Video ids Data fetched')
            time.sleep(.5)
    container3 = st.container(border=True)
    video_id_inp = container3.text_input("Video id (choose from the above)")    
    video_df = get_video_details(youtube, video_id_inp, playlist_id)
    comment_df = get_video_comments(video_id_inp)
    if container3.button("Get", key="third"):    
        if video_df is not None:
            st.header("Video data :")
            st.write(video_df)
            st.success("Video data is fetched successfully")
            st.toast('Video Data fetched')
            time.sleep(.5)
        if comment_df is not None:
            st.header("Comment data :")
            st.write(comment_df)
            st.success("Comment data is fetched successfully")
            st.toast('Comment Data fetched')
            time.sleep(.5)

#viewing the data stored in the database
elif selected_tab == "View Data":
        # Fetch channel data from the database
    # Fetch channel data from the database
    cursor.execute("SELECT * FROM channel")
    channel_rows = cursor.fetchall()
    channel_df = pd.DataFrame(channel_rows, columns=['channel_id', 'channel_name', 'channel_type', 'channel_views', 'channel_description'])

    # Fetch playlist data from the database
    cursor.execute("SELECT * FROM playlist")
    playlist_rows = cursor.fetchall()
    playlist_df = pd.DataFrame(playlist_rows, columns=['playlist_id', 'channel_id', 'playlist_name'])

    # Create a dropdown menu containing channel names
    st.title('View the stored Data :')
    selected_channel_name = st.selectbox('Select Channel Name', [''] + channel_df['channel_name'].tolist())

    # Fetch and display the playlists for the selected channel
    if selected_channel_name:
        if selected_channel_name != '':
            st.write(f"Playlists for {selected_channel_name}:")
            channel_id = channel_df.loc[channel_df['channel_name'] == selected_channel_name, 'channel_id'].values[0]
            channel_playlists = playlist_df.loc[playlist_df['channel_id'] == channel_id, 'playlist_name'].tolist()
            selected_playlist_name = st.selectbox('Select Playlist Name', [''] + channel_playlists)
            
            if selected_playlist_name:
                if selected_playlist_name != '':
                    st.write(f"Details of playlist '{selected_playlist_name}':")
                    playlist_details_df = playlist_df.loc[playlist_df['playlist_name'] == selected_playlist_name]
                    st.write(playlist_details_df)
                    
                    # Fetch and display videos for the selected playlist
                    cursor.execute("SELECT * FROM video WHERE playlist_id = %s", (playlist_details_df['playlist_id'].iloc[0],))
                    video_rows = cursor.fetchall()
                    video_df = pd.DataFrame(video_rows, columns=['video_id', 'channel_title', 'video_name', 'video_description', 'published_date', 'view_count', 'like_count', 'favorite_count', 'comment_count', 'duration', 'thumbnail', 'caption_status', 'channel_id', 'playlist_id'])
                    selected_video_name = st.selectbox('Select Video Name', [''] + video_df['video_name'].tolist())
                    
                    if selected_video_name:
                        if selected_video_name != '':
                            st.write(f"Details of video '{selected_video_name}':")
                            selected_video_details_df = video_df.loc[video_df['video_name'] == selected_video_name]
                            st.write(selected_video_details_df)

                    

    


#seperate tab for questions
elif selected_tab == 'Questions':
    with st.expander(" 1) Names of all the videos and their corresponding channels:"):
        cursor.execute("SELECT v.video_name, c.channel_name FROM video v INNER JOIN Channel c ON v.channel_title = c.channel_name")
        rows = cursor.fetchall()
        video_channel_df = pd.DataFrame(rows, columns=['Video Name', 'Channel Name'])
        st.write("1) Names of all the videos and their corresponding channels:")
        st.write(video_channel_df)

    with st.expander("2) Channels with the most number of videos and their counts:"):
        cursor.execute("SELECT channel_title, COUNT(*) AS video_count FROM video GROUP BY channel_title ORDER BY video_count DESC LIMIT 1")
        rows = cursor.fetchall()
        most_videos_df = pd.DataFrame(rows, columns=['Channel Name', 'Number of Videos'])
        st.write("2) Channels with the most number of videos and their counts:")
        st.write(most_videos_df)

    with st.expander("3) Top 10 most viewed videos and their respective channels:"):
        cursor.execute("SELECT v.video_name, v.view_count, c.channel_name FROM video v INNER JOIN Channel c ON v.channel_title = c.channel_name ORDER BY v.view_count DESC LIMIT 10")
        rows = cursor.fetchall()
        top_viewed_videos_df = pd.DataFrame(rows, columns=['Video Name', 'View Count', 'Channel Name'])
        st.write("3) Top 10 most viewed videos and their respective channels:")
        st.write(top_viewed_videos_df)

    with st.expander("4) Number of comments on each video and their corresponding names:"):
        cursor.execute("SELECT v.video_name, COUNT(c.comment_id) AS comment_count FROM video v LEFT JOIN comments c ON v.video_id = c.video_id GROUP BY v.video_name")
        rows = cursor.fetchall()
        video_comment_count_df = pd.DataFrame(rows, columns=['Video Name', 'Comment Count'])
        st.write("4) Number of comments on each video and their corresponding names:")
        st.write(video_comment_count_df)

    with st.expander("5) Videos with the highest number of likes and their corresponding channel names:"):
        cursor.execute("SELECT v.video_name, v.like_count, c.channel_name FROM video v INNER JOIN Channel c ON v.channel_title = c.channel_name ORDER BY v.like_count DESC LIMIT 10")
        rows = cursor.fetchall()
        top_liked_videos_df = pd.DataFrame(rows, columns=['Video Name', 'Like Count', 'Channel Name'])
        st.write("5) Videos with the highest number of likes and their corresponding channel names:")
        st.write(top_liked_videos_df)

    with st.expander("6) Total number of likes for each video and their corresponding video names:"):
        cursor.execute("SELECT v.video_name, SUM(v.like_count) AS total_likes FROM video v GROUP BY v.video_name")
        rows = cursor.fetchall()
        likes_dislikes_df = pd.DataFrame(rows, columns=['Video Name', 'Total Likes'])
        st.write("6) Total number of likes for each video and their corresponding video names:")
        st.write(likes_dislikes_df)

    with st.expander("7) Total number of views for each channel and their corresponding channel names:"):
        cursor.execute("SELECT channel_title, SUM(view_count) AS total_views FROM video GROUP BY channel_title")
        rows = cursor.fetchall()
        channel_views_df = pd.DataFrame(rows, columns=['Channel Name', 'Total Views'])
        st.write("7) Total number of views for each channel and their corresponding channel names:")
        st.write(channel_views_df)

    with st.expander("8) Names of all the channels that have published videos in the year 2022:"):
        cursor.execute("SELECT DISTINCT channel_name FROM Channel c INNER JOIN video v ON c.channel_name = v.channel_title WHERE YEAR(v.published_date) = 2022")
        rows = cursor.fetchall()
        channels_2022_df = pd.DataFrame(rows, columns=['Channel Name'])
        st.write("8) Names of all the channels that have published videos in the year 2022:")
        st.write(channels_2022_df)

    with st.expander("9) Average duration of all videos in each channel and their corresponding channel names:"):
        cursor.execute("SELECT channel_title, AVG(duration) AS avg_duration FROM video GROUP BY channel_title")
        rows = cursor.fetchall()
        channel_avg_duration_df = pd.DataFrame(rows, columns=['Channel Name', 'Average Duration'])
        st.write("9) Average duration of all videos in each channel and their corresponding channel names:")
        st.write(channel_avg_duration_df)

    with st.expander("10) Videos with the highest number of comments and their corresponding channel names:"):
        cursor.execute("SELECT v.video_name, COUNT(c.comment_id) AS comment_count, v.channel_title FROM video v LEFT JOIN comments c ON v.video_id = c.video_id GROUP BY v.video_name, v.channel_title ORDER BY comment_count DESC LIMIT 10")
        rows = cursor.fetchall()
        top_commented_videos_df = pd.DataFrame(rows, columns=['Video Name', 'Comment Count', 'Channel Name'])
        st.write("10) Videos with the highest number of comments and their corresponding channel names:")
        st.write(top_commented_videos_df)








        




