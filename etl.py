import os
import glob
import psycopg2
import pandas as pd
import json
from datetime import datetime
from sql_queries import *
from io import StringIO

index_songplay = 0
columns_data_types = {}

def get_columns_data_types(cur):
    global columns_data_types
    cur.execute(data_types_select)
    
    row = cur.fetchone()
    while row:
        if row[0] in columns_data_types.keys():
            columns_data_types[row[0]][row[1]] = row[2]
        else:
            columns_data_types[row[0]] ={}
            columns_data_types[row[0]][row[1]] = row[2]
        row = cur.fetchone()

def data_quality_checks(table, df):
    #check if data is a list for one row
    global columns_data_types
    
    if table in columns_data_types.keys():
        for key, value in columns_data_types[table].items():
                
            if value == "character varying": 
                if(str(df[key].dtypes) != "object"):
                    df[key] = df[key].astype("str") 
            elif value == "integer":
                if(str(df[key].dtypes).find("int") == -1):
                    df[key] = df[key].astype("int")
            elif value =="numeric":
                if(str(df[key].dtypes).find("float") == -1):
                    df[key] = df[key].astype("float")
            elif value == "timestamp without time zone":
                if(str(df[key].dtypes).find("datetime64") == -1):
                    df[key] = pd.to_datetime(df[key], infer_datetime_format=True)
    else:
        valid=False
        raise Exception("Sorry, table provided was incorrect")
    return (df)
    #check if data is dataframe for a bunch of rows


def process_song_file(cur, filepath, conn):
    # open song file
    f = open(filepath)
    data = json.load(f)
    df = pd.DataFrame(data, index=[0])

    # insert artist record
    artist_data = df.filter(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    artist_data.rename(columns={"artist_name": "name", "artist_location": "location", "artist_latitude": "latitude", "artist_longitude": "longitude"}, inplace=True)
    artist_data = data_quality_checks("artists", artist_data)
    artist_data = artist_data.values
    artist_data = artist_data[0]
    artist_data = list(artist_data)
    cur.execute(artist_table_insert, artist_data)
    
     # insert song record
    song_data = df.filter(['song_id', 'title', 'artist_id', 'year', 'duration'])
    song_data = data_quality_checks("songs", song_data)
    song_data = song_data.values
    song_data = song_data[0]
    song_data = list(song_data)
    cur.execute(song_table_insert, song_data)
    
def copy_from_stringio(conn, df, table, index_label):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """

    df.drop_duplicates(subset=index_label, 
                     keep = 'last', inplace = True)
#     if(table =="users"):
#         pd.set_option("display.max_rows", None, "display.max_columns", None)
#         print(df)
#         return
    
    
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
#     print(buffer.read())
    
    try:
        
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
#     raise Exception("Sorry, no numbers below zero")
    cursor.close()


def process_log_file(cur, filepath, conn):
    global index_songplay
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    has_nextSong = df['page'] == "NextSong"
    df = df[has_nextSong]

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda x: datetime.fromtimestamp(x/1000))
    timestamp = df['ts'].apply(lambda x: x/1000)
    df['ts'] = pd.to_datetime(timestamp, unit='s')
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({c:d for c,d in zip(column_labels, time_data)})
    time_df = data_quality_checks("time", time_df)
#   copy_from_stringio(conn, time_df, 'time', 'start_time')
#   old implementation isert row by row in time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(['userId', 'firstName', 'lastName', 'gender', 'level'])
    user_df.rename(columns={"userId": "user_id", "firstName": "first_name", "lastName": "last_name"}, inplace=True)
    user_df = data_quality_checks("users", user_df)
#         copy_from_stringio(conn, user_df, 'users', ['user_id', 'first_name', 'last_name'])
#     old implementation insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    songplay_df = pd.DataFrame(columns=['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id','location', 'user_agent']) #
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            # insert songplay record
        songplay_data = {'songplay_id': index_songplay, 'start_time': row.ts, 'user_id':row.userId, 'level':row.level, 'song_id':songid, 'artist_id':artistid, 'session_id':row.sessionId, 'location':row.location, 'user_agent':row.userAgent} #,
        songplay_df = songplay_df.append(songplay_data, ignore_index = True)
#         old insert songplay
#         cur.execute(songplay_table_insert, songplay_data)
        index_songplay += 1
#     songplay_df = data_quality_checks("songplays", songplay_df)
    print(songplay_df['level'].isnull().sum())
#    songplay_df = songplay_df.assign(location='France')
#   songplay_df = songplay_df.assign(user_agent='Mozilla')
    copy_from_stringio(conn, songplay_df, 'songplays', 'songplay_id')


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, conn)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    cur = conn.cursor()
    get_columns_data_types(cur)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()