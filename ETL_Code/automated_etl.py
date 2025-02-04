import pandas as pd
import os
import glob
from datetime import datetime
import mysql.connector
from nltk.corpus import stopwords
import pickle
import re 
from tabulate import tabulate
def menu_utama():
    print('''
    =================Program ETL By Novan Janis=================
    1. Do ETL
    0. End Program
    ''')
    try:
        pilihmenu_utama=int(input('Choose the menu (1/0): '))
        if pilihmenu_utama==1:
            first_etl()
        elif pilihmenu_utama==0:
            print('Program has ended. Have a nice day. \U0001f604 ')
        else:
           os.system('cls')
           print('Wrong input. Try again.')
           menu_utama()
    except ValueError:
        os.system('cls')
        print('Wrong input. Try again.')
        menu_utama()
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    port = "3360",
    database="dwh_ta_test"
)
with open('naive_bayes_model.pkl', 'rb') as file:
    model = pickle.load(file)
with open('tfidf_vectorizer.pkl', 'rb') as file:
    tfidf_vectorizer = pickle.load(file)
list_stopwords = stopwords.words('indonesian')
list_stopwords.extend(['yg', 'dgn', 'dg', 'dlm', 'dr',
    'tp', 'bgt', 'jg', 'ga', 'tdk',
    'klo', 'gw', 'lu', 'gak', 'emg',
    'jd', 'gmn', 'dpt', 'bs', 'utk','sllu','anjir','nya'])
other_stopword = pd.read_csv('stopwords-id.txt',names= ["stopwords"], header = None)
list_stopwords.extend(other_stopword['stopwords'][0].split(' '))
list_stopwords = set(list_stopwords)
exception_words = set(['jauh','dekat','besar','kecil', 'keluar','masuk'])
def remove_stopwords(text):
    words = text.split()
    words = [word for word in words if word not in list_stopwords or word in exception_words]
    if re.fullmatch(r'@\w+', text.strip()):
        return ''
    return ' '.join(words)


def sentiment_predict(text, model, vectorizer):
    text = text.lower()
    text = remove_stopwords(text)
    if not text:
        return 'neutral'
    text_vectorized = vectorizer.transform([text.lower().strip()])
    sentiment = model.predict(text_vectorized)[0]
    return sentiment
def apply_sentiment(df, model, vectorizer):
    sentiment_column = []
    for _, row in df.iterrows():
        if 'Text' in row:
            text = row['Text']
        elif 'textOriginal' in row:
            text = row['textOriginal']
        else:
            text = None
        if text:
            sentiment = sentiment_predict(text, model, vectorizer)
        else:
            sentiment = 'neutral'
        sentiment_column.append(sentiment)
    df['sentiment'] = sentiment_column
    return df

def first_etl():
    dir = input('Input Extraction directory path: ')
    print('Menjalankan ETL...')
    youtube_dir = os.path.join(dir, 'youtube') 
    dfs_youtube = []

    for subdir, _, _ in os.walk(youtube_dir):
        csv_files = glob.glob(os.path.join(subdir, '*.csv'))
        for file in csv_files:
            df = pd.read_csv(file)          
            directory_name = os.path.basename(subdir)
            parent_directory_name = os.path.basename(os.path.dirname(subdir))           
            df['wisata'] = directory_name
            df['socmed'] = parent_directory_name
            if 'sentiment' not in df.columns:
                df = apply_sentiment(df, model, tfidf_vectorizer)
    
            df['sentiment'] = df['sentiment'].map({'positive': 1, 'negative': -1, 'neutral': 0})
            dfs_youtube.append(df)
    df_youtube = pd.concat(dfs_youtube, ignore_index=True)
    print('YouTube Data successfully Extracted with total rows:', len(df_youtube),'\n')
    
    tiktok_dir = os.path.join(dir, 'tiktok')  
    dfs_tiktok = []

    for subdir, _, _ in os.walk(tiktok_dir):
        csv_files = glob.glob(os.path.join(subdir, '*.csv'))
        for file in csv_files:
            df = pd.read_csv(file)           
            directory_name = os.path.basename(subdir)
            parent_directory_name = os.path.basename(os.path.dirname(subdir))           
            df['wisata'] = directory_name
            df['socmed'] = parent_directory_name   
            if 'sentiment' not in df.columns:
                df = apply_sentiment(df, model, tfidf_vectorizer)         
            df['sentiment'] = df['sentiment'].map({'positive': 1, 'negative': -1, 'neutral': 0})
    
            dfs_tiktok.append(df)
    df_tiktok = pd.concat(dfs_tiktok, ignore_index=True)
    print('tiktok Data successfully Extracted with total rows:', len(df_tiktok), '\n')


    df_youtube = df_youtube.rename(columns={
        'id': 'id',
        'textOriginal': 'comment_text',
        'authorChannelId': 'user_id',
        'authorDisplayName': 'username',
        'likeCount': 'num_likes',
        'publishedAt': 'timestamp',
        'sentiment': 'sentiment'
    })

    df_tiktok = df_tiktok.rename(columns={
        'Comment_ID': 'id',
        'Author_ID': 'user_id',
        'Author': 'username',
        'Text': 'comment_text',
        'Likes': 'num_likes',
        'Comment_Date': 'timestamp',
        'sentiment': 'sentiment'
    })

    df_comment_data = pd.concat([df_youtube, df_tiktok], ignore_index=True)

    


    dir_tourism = os.path.join(dir + '/Tourism')
    json_files = glob.glob(os.path.join(dir_tourism, '*.json'))

    if not json_files:
        cursor = mydb.cursor(dictionary=True)
        cursor.execute('SELECT * FROM dim_tourism')
        rows = cursor.fetchall()
        df_dim_tourism = pd.DataFrame(rows)
        print('Tourism Data fetched from database with total rows:', len(df_dim_tourism), '\n')
        
    else:
        df_tourism_source = pd.concat([pd.read_json(file) for file in json_files], ignore_index=True)
        if not df_tourism_source.empty:
            df_tourism_source['regency'] = df_tourism_source['regency'].str.replace('Kabupaten ', '', regex=False)
            df_tourism_source['regency'] = df_tourism_source['regency'].str.replace('Kota ', '', regex=False)
            df_tourism_source.sort_values(by='reviews', ascending=False, inplace=True)
            print('Tourism Data successfully Extracted with total rows:', len(df_tourism_source), '\n')
    


        df_dim_tourism_regency = pd.DataFrame()
        df_dim_tourism_regency['regency'] = df_tourism_source['regency'].unique()
        df_dim_tourism_regency['regency_id'] = ['rg{:02d}'.format(i) for i in range(1, len(df_dim_tourism_regency) + 1)]

        df_dim_tourism_regency = df_dim_tourism_regency[['regency_id'] + ['regency']]
        print('Dimension table for regency data successfully created with total rows:', len(df_dim_tourism_regency)) 

        df_dim_tourism_category = pd.DataFrame()
        df_dim_tourism_category['category'] = df_tourism_source['category'].unique()
        df_dim_tourism_category['category_id'] = ['ctg{:02d}'.format(i) for i in range(1, len(df_dim_tourism_category) + 1)]

        df_dim_tourism_category = df_dim_tourism_category[['category_id'] + ['category']]
        print('Dimension table for category data successfully created with total rows:', len(df_dim_tourism_category))

        df_dim_tourism = pd.DataFrame()
        df_dim_tourism['tourism_id'] = ['tsm{:02d}'.format(i) for i in range(1, len(df_tourism_source) + 1)]
        regency_to_id = dict(zip(df_dim_tourism_regency['regency'], df_dim_tourism_regency['regency_id']))
        df_dim_tourism['regency_id'] = df_tourism_source['regency'].map(regency_to_id)
        category_to_id = dict(zip(df_dim_tourism_category['category'], df_dim_tourism_category['category_id']))
        df_dim_tourism['category_id'] = df_tourism_source['category'].map(category_to_id)
        df_dim_tourism['tourism_name'] = df_tourism_source['title']
        df_dim_tourism['tourism_location'] = df_tourism_source['address']
        print('Dimension table for tourism data successfully created with total rows:', len(df_dim_tourism))

    data = {
        'socmed_id': ['sm1', 'sm2'],
        'socmed_name': ['youtube', 'tiktok'],
    }
    df_dim_socmed = pd.DataFrame(data)
    print('Dimension table for social media data successfully created with total rows:', len(df_dim_socmed))

    df_dim_user = pd.DataFrame()
    df_dim_user['user_id'] = df_comment_data['user_id'].unique()
    df_dim_user['username'] = df_comment_data['username'].unique()
    df_dim_user['username'] = df_dim_user['username'].str.replace('@', '', regex=False)
    print('Dimension table for user data successfully created with total rows:', len(df_dim_user))


    def convert_timestamp(timestamp):
        try:
            return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            try:
                return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').date()
            except ValueError:
                return None
    df_comment_data['timestamp'] = df_comment_data['timestamp'].apply(convert_timestamp)
    df_comment_data['timestamp'] = pd.to_datetime(df_comment_data['timestamp'], errors='coerce')
    df_comment_data = df_comment_data[df_comment_data['timestamp'].dt.year >= 2019]

    df_dim_date = pd.DataFrame()
    df_dim_date['date_id'] = df_comment_data['timestamp'].unique()
    df_dim_date['date_id'] = df_dim_date['date_id'].apply(lambda x: int(x.strftime('%Y%m%d')))
    df_dim_date['year'] = df_dim_date['date_id'].apply(lambda x: int(str(x)[:4]))
    df_dim_date['month'] = df_dim_date['date_id'].apply(lambda x: int(str(x)[4:6]))
    df_dim_date['day'] = df_dim_date['date_id'].apply(lambda x: int(str(x)[6:]))

    if df_dim_date['date_id'].duplicated().sum() > 0:
        print('Warning: duplicated data detected')
    else:
        print('Dimension table for date data successfully created with total rows:', len(df_dim_date))


    df_fact_table = df_comment_data
    if 'username' in df_fact_table.columns:
        df_fact_table.drop(['username'], axis=1, inplace=True)
    if 'id' in df_fact_table.columns:
        df_fact_table.drop(['id'], axis=1, inplace=True)
    if 'wisata' in df_fact_table.columns:
        mapping = {}
        for i, row in df_dim_tourism.iterrows():
            for part in row['tourism_name'].split():
                if part not in mapping:
                    mapping[part] = row['tourism_id']


        def find_tourism_id(location_name):
            for part in location_name.split():
                if part in mapping:
                    return mapping[part]
            return None


        df_fact_table['tourism_id'] = df_fact_table['wisata'].apply(find_tourism_id)
        if df_fact_table['tourism_id'].isnull().sum() == 0:
            df_fact_table.drop(columns=['wisata'], inplace=True)

    if 'socmed' in df_fact_table.columns:
        socmed_to_id = dict(zip(df_dim_socmed['socmed_name'], df_dim_socmed['socmed_id']))
        df_fact_table['socmed_id'] = df_fact_table['socmed'].map(socmed_to_id)
        if df_fact_table['socmed_id'].isnull().sum() == 0:
            df_fact_table.drop(columns=['socmed'], inplace=True)

    if 'timestamp' in df_fact_table.columns:
        df_fact_table['date_id'] = df_fact_table['timestamp'].apply(lambda x: int(x.strftime('%Y%m%d')))

    print('Fact table successfully created with total rows:', len(df_fact_table), '\n')


    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS dim_social_media (socmed_id VARCHAR(255) PRIMARY KEY, socmed_name VARCHAR(255))")
    mycursor.execute("SELECT COUNT(*) FROM dim_social_media")
    initial_count_dim_socmed = mycursor.fetchone()[0]
    if initial_count_dim_socmed == 0:
        for index, row in df_dim_socmed.iterrows():
            mycursor.execute("INSERT INTO dim_social_media (socmed_id, socmed_name) VALUES (%s, %s)", (row['socmed_id'], row['socmed_name']))
        mydb.commit()
        mycursor.execute("SELECT COUNT(*) FROM dim_social_media")
        final_count_dim_socmed = mycursor.fetchone()[0]
        print("Successfully load data into dim_social_media table with total rows:", final_count_dim_socmed - initial_count_dim_socmed)
        print('new data added:')
        new_data = df_dim_socmed.head()
        print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')
    else:
        mycursor.execute("SELECT COUNT(*) FROM dim_social_media")
        final_count_dim_socmed = mycursor.fetchone()[0]
        print('no updated data for dim_social_media table \n\n')


    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS dim_user (user_id VARCHAR(255) PRIMARY KEY, username VARCHAR(255))")
    mycursor.execute("SELECT COUNT(*) FROM dim_user")
    initial_count_dim_user = mycursor.fetchone()[0]
    for index, row in df_dim_user.iterrows():
        mycursor.execute("INSERT IGNORE INTO dim_user (user_id, username) VALUES (%s, %s)", (row['user_id'], row['username']))
    mydb.commit()
    mycursor.execute("SELECT COUNT(*) FROM dim_user")
    final_count_dim_user = mycursor.fetchone()[0]
    add_user = final_count_dim_user - initial_count_dim_user
    if add_user == 0:
        print('no updated data for dim_user table \n\n')
    else:
        print("Successfully load data into dim_user table with total rows:", add_user)
        print('new data added:')
        new_data = df_dim_user.head()
        print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')

    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS dim_date (date_id INT PRIMARY KEY, day INT, month INT, year INT)")
    mycursor.execute("SELECT COUNT(*) FROM dim_date")
    initial_count_dim_date = mycursor.fetchone()[0]
    for index, row in df_dim_date.iterrows():
        mycursor.execute("INSERT IGNORE INTO dim_date (date_id, day, month, year) VALUES (%s, %s, %s, %s)", (int(row['date_id']), int(row['day']), int(row['month']), int(row['year'])))
    mydb.commit()
    mycursor.execute("SELECT COUNT(*) FROM dim_date")
    final_count_dim_date = mycursor.fetchone()[0]
    add_date = final_count_dim_date - initial_count_dim_date
    if add_date == 0:
        print('no updated data for dim_date table \n\n')
    else:
        print("Successfully load data into dim_date table with total rows:", add_date)
        print('new data added:')
        df_dim_date['date_id'] = df_dim_date['date_id'].astype(str)
        new_data = df_dim_date.head()
        print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')

    if json_files:
        mycursor = mydb.cursor()
        mycursor.execute("CREATE TABLE IF NOT EXISTS dim_tourism_regency (regency_id VARCHAR(255) PRIMARY KEY, regency VARCHAR(255))")
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_regency")
        initial_count_dim_tourism_regency = mycursor.fetchone()[0]
        for index, row in df_dim_tourism_regency.iterrows():
            mycursor.execute("INSERT IGNORE INTO dim_tourism_regency (regency_id, regency) VALUES (%s, %s)", (row['regency_id'], row['regency']))
        mydb.commit()
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_regency")
        final_count_dim_tourism_regency = mycursor.fetchone()[0]
        add_dim_tourism_regency = final_count_dim_tourism_regency - initial_count_dim_tourism_regency
        if add_dim_tourism_regency == 0:
            print('no updated data for dim_tourism_regency table \n\n')
        else:
            print("Successfully load data into dim_tourism_regency table with total rows:", add_dim_tourism_regency)
            print('new data added:')
            new_data = df_dim_tourism_regency.head()
            print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')

        mycursor = mydb.cursor()
        mycursor.execute("CREATE TABLE IF NOT EXISTS dim_tourism_category (category_id VARCHAR(255) PRIMARY KEY, category VARCHAR(255))")
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_category")
        initial_count_dim_tourism_category = mycursor.fetchone()[0]
        for index, row in df_dim_tourism_category.iterrows():
            mycursor.execute("INSERT IGNORE INTO dim_tourism_category (category_id, category) VALUES (%s, %s)", (row['category_id'], row['category']))
        mydb.commit()
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_category")
        final_count_dim_tourism_category = mycursor.fetchone()[0]
        add_dim_tourism_category = final_count_dim_tourism_category - initial_count_dim_tourism_category
        if add_dim_tourism_category == 0:
            print('no updated data for dim_tourism_category table \n\n')
        else:
            print("Successfully load data into dim_tourism_category table with total rows:", add_dim_tourism_category)
            print('new data added:')
            new_data = df_dim_tourism_category.head()
            print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')

        mycursor = mydb.cursor()
        mycursor.execute("CREATE TABLE IF NOT EXISTS dim_tourism (tourism_id VARCHAR(255) PRIMARY KEY, regency_id VARCHAR(255), category_id VARCHAR(255), tourism_name VARCHAR(255), tourism_location VARCHAR(255),FOREIGN KEY (regency_id) REFERENCES dim_tourism_regency(regency_id), FOREIGN KEY (category_id) REFERENCES dim_tourism_category(category_id))")
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism")
        initial_count_dim_tourism = mycursor.fetchone()[0]
        for index, row in df_dim_tourism.iterrows():
            mycursor.execute("INSERT IGNORE INTO dim_tourism (tourism_id, regency_id, category_id, tourism_name, tourism_location) VALUES (%s, %s, %s, %s, %s)", (row['tourism_id'], row['regency_id'], row['category_id'], row['tourism_name'], row['tourism_location']))
        mydb.commit()
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism")
        final_count_dim_tourism = mycursor.fetchone()[0]
        add_dim_tourism = final_count_dim_tourism - initial_count_dim_tourism
        if add_dim_tourism == 0:
            print('no updated data for dim_tourism table \n\n')
        else:
            print("Successfully load data into dim_tourism table with total rows:", len(df_dim_tourism))
            print('new data added:')
            def truncate_text(text, max_length=15):
                return text if len(text) <= max_length else text[:max_length] + '...'
            df_dim_tourism['tourism_location'] = df_dim_tourism['tourism_location'].apply(truncate_text)
            new_data = df_dim_tourism.head()
            print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')
    else:
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_regency")
        final_count_dim_tourism_regency = mycursor.fetchone()[0]
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism_category")
        final_count_dim_tourism_category = mycursor.fetchone()[0]
        mycursor.execute("SELECT COUNT(*) FROM dim_tourism")
        final_count_dim_tourism = mycursor.fetchone()[0]
        print('no updated data for dim_tourism_category table\n\n')
        print('no updated data for dim_tourism_regency table\n\n')
        print('no updated data for dim_tourism table\n\n')

    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS fact_table(socmed_id VARCHAR(255), user_id VARCHAR(255), date_id INT, tourism_id VARCHAR(255), comment_text TEXT, num_likes INT, sentiment VARCHAR(255), timestamp DATE, FOREIGN KEY (socmed_id) REFERENCES dim_social_media(socmed_id), FOREIGN KEY (user_id) REFERENCES dim_user(user_id), FOREIGN KEY (date_id) REFERENCES dim_date(date_id), FOREIGN KEY (tourism_id) REFERENCES dim_tourism(tourism_id))")
    mycursor.execute("SELECT COUNT(*) FROM fact_table")
    initial_count_fact_table = mycursor.fetchone()[0]
    for index, row in df_fact_table.iterrows():
        mycursor.execute("INSERT INTO fact_table (socmed_id, user_id, date_id, tourism_id, comment_text, num_likes, sentiment, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row['socmed_id'], row['user_id'], row['date_id'], row['tourism_id'], row['comment_text'], row['num_likes'], row['sentiment'], row['timestamp']))
    mydb.commit()
    mycursor.execute("SELECT COUNT(*) FROM fact_table")
    final_count_fact_table = mycursor.fetchone()[0]
    add_fact_table = final_count_fact_table - initial_count_fact_table
    if add_fact_table == 0:
        print('no updated data for fact_table table \n\n')
    else:
        print("Successfully load data into fact_table table with total rows:", add_fact_table)
        print('new data added:')
        def truncate_text(text, max_length=15):
            return text if len(text) <= max_length else text[:max_length] + '...'
        df_fact_table['comment_text'] = df_fact_table['comment_text'].apply(truncate_text)
        new_data = df_fact_table.head()
        print(tabulate(new_data, headers='keys', tablefmt='fancy_outline'),'\n\n')
    print('\n')
    print('data warehouse latest records count:')
    print('dim_tourism:', final_count_dim_tourism)
    print('dim_tourism_category:', final_count_dim_tourism_category)
    print('dim_tourism_regency:', final_count_dim_tourism_regency)
    print('dim_user:', final_count_dim_user)
    print('dim_date:', final_count_dim_date)
    print('dim_social_media:', final_count_dim_socmed)
    print('fact_table:', final_count_fact_table)

    print('ETL process has finished!')
    menu_utama()
menu_utama()