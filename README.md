<h1>Summary of the project, how to run the Python scripts, and an explanation of the files in the repository</h1>

First requirement is to have a postgresql database server opened with a dbname sparkifydb and user=student, password=student.
The second requirement is to have python 3 version installed to execute the scripts create_tables.py, etl.py and analytics.

You can test a version of code writed in etl.py in etl.ipynb. In this file I've changed the order I add data in the songs and artists table. Artists are entered first because in the songs table I need to add a reference of artist_id

First step Open a terminal, assure in the right folder and execute the command python3 create_tables.py
Second step: Execute the command python3 etl.py
You can use test.ipynb notebook to test the database tables or analytics.ipynb to make some analysis with the data new added in the database.

<h2>1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.</h2>
The client wants to collect songs and user activities from their new streaming app.
The purpose of collecting data is to understand better the preferences of their users. Who are the users?(their name, gender, location and if they are currently paying users, and the broswer(user agent) they use. Also we are interested what are their favourite songs and when the users prefer to listening that songs. In this study we are also interested about artists and their specific data like name and location, latitude and longitude.

Based on the data we collect we can imagine different scenarios of using it. 

Some cases would be to add new songs from specific artists which are the most wanted by our users. To add a section with recommended songs in the app. To create an event in a location where one or more artists have the most fans in our app or to be sponsor at this kind of event or sell those data to other parties directly interested in this.(Event organizers, radio channels).

<h2>2.State and justify your database schema design and ETL pipeline.</h2>

I set the primary keys for each of the tables.

I've choosed data types for the columns in tables based on their specific content: timestamp, int, numeric, varchar

In etl.py I joined data from artists and songs tables to check a match with the rows in log data files based on the artist name,  song title and song duration. As I saw the files in song_data folder are parsed in a certain order(A, B, C alphabetically in this case and for the name of the files the same ) and I suppose the last version of the song, artist or user is also the correct one(the last updated) so I used UPSERT for songs, artists, users, and time tables.

For the songplay table I added songplay_id as the primary key. The same like above I used UPSERT instead of insert for this table. Because the FOREIGN KEYS artists_id and song_id have null values I removed those constraints(FOREIGN KEYS).PLease provide me a solution if it exist to add Foreign Keys with posibility of the values to be NULL

For copy_from I tested it also with other tables like artists and users. Can you provide to me an UPSERT method for copy_from? It can be used only if I'm not going to add duplicate data.

I used the function data_quality_checks to compare data types in dataframe with the data types of the corespondent columns in the database. So when I found a data type in the database as integer or numeric I transform the entire column of the dataframe to int or float.


<h2>3. Provide example queries and results for song play analysis.</h2>

Select the location of the users and the number of users grouped by location
SELECT location, COUNT(DISTINCT user_id) FROM songplays GROUP BY location;


Group your users by gender:
SELECT gender, COUNT(user_id) FROM users GROUP BY gender;


Get data for the database By Joining the Fact Table songplays with dimension tables artists, songs, users and time
SELECT songplays.artist_id,  songplays.song_id, songplays.start_time, songplays.location, songs.duration FROM songplays JOIN  artists ON songplays.artist_id=artists.artist_id JOIN songs ON songplays.song_id=songs.song_id JOIN time ON time.start_time=songplays.start_time;







