from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text # Import text for raw SQL
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy.dialects.postgresql import insert  # สำคัญ!
from sqlalchemy import MetaData, Table
# Default args for the DAG
default_args = {
    'owner': 'four',
    'retries': 1,
}

# Define the DAG
dag = DAG(
    dag_id='movie_dag',
    default_args=default_args,
    # Set the start date to a past date for the DAG to be scheduled immediately
    start_date=datetime(2023, 1, 1), # Changed to a fixed past date for consistency
    schedule="0 0 * * *",  # Daily at midnight UTC
    catchup=False, # Do not run for past missed schedules
    description='ETL from TMDB to PostgreSQL (Airflow 3)',
)

# Connection ID for PostgreSQL, as defined in Airflow Connections
SQL_CONN = "postgres_default"
# Base URL for the TMDB Discover Movie API
BASE_URL = "https://api.themoviedb.org/3/discover/movie"
# TMDB Genres API URL
GENRE_API_URL = "https://api.themoviedb.org/3/genre/movie/list?language=en"

def extract_tmdb_data(**context):
    """
    Extracts movie data from the TMDB API.
    Fetches data from the first 5 pages and pushes it to XCom.
    """
    all_movies = []
    # Loop through the first 5 pages of the API results
    for page in range(1, 6):  
        params = {
            "include_adult": "false",
            "include_video": "false",
            "language": "en-US",
            "page": page,
            "sort_by": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        }
        headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMDlhNjZhMmZkN2MwMTNmMjIxZmYzMmQ3ZjE5YWIwOSIsIm5iZiI6MTc1MjMyMDQ2My44MzIsInN1YiI6IjY4NzI0OWNmMjc2NmQ3M2NlNjcwNGQzMSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.yGOVRlnVJwoqqE-b-lvBjXBi4AvRAkAbYz06Sj7__VA"
        }

        response = requests.get(BASE_URL, params=params, headers=headers)
        
        # Check if the API request was successful
        if response.status_code == 200:
            data = response.json()
            all_movies.extend(data.get("results", [])) # Add movie results to the list
        else:
            # Raise an exception if the API call fails
            raise Exception(f"TMDB API Error: {response.status_code} - {response.text}")
    
    # Push the list of all movies to XCom, making it available for downstream tasks
    context['ti'].xcom_push(key='movies', value=all_movies)

def extract_genres_data(**context):
    """
    Extracts movie genre data from the TMDB API.
    Pushes the list of genres to XCom.
    """
    headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMDlhNjZhMmZkN2MwMTNmMjIxZmYzMmQ3ZjE5YWIwOSIsIm5iZiI6MTc1MjMyMDQ2My44MzIsInN1YiI6IjY4NzI0OWNmMjc2NmQ3M2NlNjcwNGQzMSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.yGOVRlnVJwoqqE-b-lvBjXBi4AvRAkAbYz06Sj7__VA"
    }


    response = requests.get(GENRE_API_URL, headers=headers)

    if response.status_code == 200:
        data = response.json()
        genres = data.get("genres", [])
        context['ti'].xcom_push(key='genres_list', value=genres)
    else:
        raise Exception(f"TMDB Genre API Error: {response.status_code} - {response.text}")

def transform_data(**context):
    """
    Transforms the extracted movie data.
    Cleans the data, filters for non-adult movies, removes duplicates,
    converts release dates, and handles missing overviews.
    Pushes cleaned movie data and movie-genre mapping to XComs.
    """
    # Pull movie data from XCom pushed by the 'extract_tmdb' task
    movies = context['ti'].xcom_pull(key='movies', task_ids='extract_tmdb')
    df = pd.DataFrame(movies)

    # Data Cleaning and Transformation
    df = df[df['adult'] == False] # Filter out adult movies
    df = df.drop_duplicates(subset='id') # Remove duplicate movies based on ID
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce') # Convert to datetime, coerce errors to NaT
    df.fillna({'overview': 'No description'}, inplace=True) # Fill missing overviews

    # Prepare data for the 'movies' table
    movie_df = df[[
        'id', 'title', 'overview', 'release_date',
        'original_language', 'popularity', 'vote_average',
        'vote_count', 'adult', 'poster_path', 'backdrop_path'
    ]]
    # Push the cleaned movie DataFrame to XCom
    context['ti'].xcom_push(key='cleaned_movies_df', value=movie_df)
    
    # Prepare data for the 'movie_genres' table (mapping movies to genres)
    genre_records = []
    for _, row in df.iterrows():
        movie_id = row['id']
        # Iterate through genre IDs for each movie
        for genre_id in row.get('genre_ids', []):
            genre_records.append({'movie_id': movie_id, 'genre_id': genre_id})

    genre_df = pd.DataFrame(genre_records)
    
    # Push the movie-genre DataFrame to XCom
    context['ti'].xcom_push(key='movie_genres_df', value=genre_df)

def load_genres_to_postgres(**context):
    conn = BaseHook.get_connection(SQL_CONN)
    engine = create_engine(f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    genres_list = context['ti'].xcom_pull(key='genres_list', task_ids='extract_genres')
    genres_df = pd.DataFrame(genres_list)

    metadata = MetaData()
    metadata.reflect(bind=engine)
    genres_table = Table('genres', metadata, autoload_with=engine)

    with engine.begin() as connection:
        for _, row in genres_df.iterrows():
            stmt = insert(genres_table).values(row.to_dict())
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['id'],  # ต้องมี PRIMARY KEY หรือ UNIQUE(id)
                set_={col: stmt.excluded[col] for col in row.to_dict() if col != 'id'}
            )
            connection.execute(upsert_stmt)
def load_to_postgres(**context):
    conn = BaseHook.get_connection(SQL_CONN)
    engine = create_engine(f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    
    movie_df = context['ti'].xcom_pull(key='cleaned_movies_df', task_ids='transform_movies')
    movie_genres_df = context['ti'].xcom_pull(key='movie_genres_df', task_ids='transform_movies')

    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Load table schema
    movies_table = Table('movies', metadata, autoload_with=engine)
    movie_genres_table = Table('movie_genres', metadata, autoload_with=engine)

    with engine.begin() as connection:
        # --- UPSERT MOVIES ---
        for _, row in movie_df.iterrows():
            stmt = insert(movies_table).values(row.to_dict())
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['id'],  # ใช้ primary key หรือ unique key
                set_={col: stmt.excluded[col] for col in row.to_dict() if col != 'id'}
            )
            connection.execute(upsert_stmt)

        # --- UPSERT MOVIE_GENRES ---
        for _, row in movie_genres_df.iterrows():
            stmt = insert(movie_genres_table).values(row.to_dict())
            upsert_stmt = stmt.on_conflict_do_nothing()  # ไม่อัปเดต เพราะ movie_id+genre_id ซ้ำไม่น่ามีข้อมูลเปลี่ยน
            connection.execute(upsert_stmt)

# Task to extract genre data from TMDB
extract_genres_task = PythonOperator(
    task_id='extract_genres',
    python_callable=extract_genres_data,
   
    dag=dag,
)

# Task to transform the extracted movie data
transform_task = PythonOperator(
    task_id='transform_movies',
    python_callable=transform_data,
   
    dag=dag,
)

# Task to load genre data into PostgreSQL
load_genres_task = PythonOperator(
    task_id='load_genres_to_postgres',
    python_callable=load_genres_to_postgres,
   
    dag=dag,
)

# Task to load transformed movie data and movie_genres data into PostgreSQL
load_movies_and_genres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
   
    dag=dag,
)
extract_movie_task = PythonOperator(
    task_id='extract_tmdb',
    python_callable=extract_tmdb_data,
    dag=dag,
)

# Task to extract genre data from TMDB


# --- Task Pipeline (Dependencies) ---
# Define the order of execution to ensure data integrity
# 1. Extract raw data (movies and genres can be parallel)
extract_movie_task
extract_genres_task

# 2. Transform movie data (depends on movie extraction)
extract_movie_task >> transform_task

# 3. Load genres (depends on genre extraction)
extract_genres_task >> load_genres_task

# 4. Load movies and movie_genres (depends on transformed movies AND loaded genres)
# This ensures 'genres' table is populated before 'movie_genres' is loaded.
[transform_task, load_genres_task] >> load_movies_and_genres_task
