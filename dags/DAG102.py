from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text 
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy import MetaData, Table

default_args = {
    'owner': 'four',
    'retries': 1,
}

# Define the DAG
dag = DAG(
    dag_id='movie_dag',
    default_args=default_args,
  
    start_date=datetime(2023, 1, 1),
    schedule="0 0 * * *", 
    catchup=False, 
    description='ETL from TMDB to PostgreSQL (Airflow 3)',
)


SQL_CONN = "postgres_default"

BASE_URL = "https://api.themoviedb.org/3/discover/movie"

GENRE_API_URL = "https://api.themoviedb.org/3/genre/movie/list?language=en"

def extract_tmdb_data(**context):
   
    all_movies = []
   
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
        
        
        if response.status_code == 200:
            data = response.json()
            all_movies.extend(data.get("results", [])) 
        else:
           
            raise Exception(f"TMDB API Error: {response.status_code} - {response.text}")
    
   
    context['ti'].xcom_push(key='movies', value=all_movies)

def extract_genres_data(**context):
    
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
   
    movies = context['ti'].xcom_pull(key='movies', task_ids='extract_tmdb')
    df = pd.DataFrame(movies)

    
    df = df[df['adult'] == False]
    df = df.drop_duplicates(subset='id') 
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce') 
    df.fillna({'overview': 'No description'}, inplace=True) 

   
    movie_df = df[[
        'id', 'title', 'overview', 'release_date',
        'original_language', 'popularity', 'vote_average',
        'vote_count', 'adult', 'poster_path', 'backdrop_path'
    ]]
    
    context['ti'].xcom_push(key='cleaned_movies_df', value=movie_df)
    
    
    genre_records = []
    for _, row in df.iterrows():
        movie_id = row['id']
       
        for genre_id in row.get('genre_ids', []):
            genre_records.append({'movie_id': movie_id, 'genre_id': genre_id})

    genre_df = pd.DataFrame(genre_records)
    
    
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
                index_elements=['id'],  
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
      
        for _, row in movie_df.iterrows():
            stmt = insert(movies_table).values(row.to_dict())
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['id'],  
                set_={col: stmt.excluded[col] for col in row.to_dict() if col != 'id'}
            )
            connection.execute(upsert_stmt)

        for _, row in movie_genres_df.iterrows():
            stmt = insert(movie_genres_table).values(row.to_dict())
            upsert_stmt = stmt.on_conflict_do_nothing() 
            connection.execute(upsert_stmt)


extract_genres_task = PythonOperator(
    task_id='extract_genres',
    python_callable=extract_genres_data,
   
    dag=dag,
)


transform_task = PythonOperator(
    task_id='transform_movies',
    python_callable=transform_data,
   
    dag=dag,
)


load_genres_task = PythonOperator(
    task_id='load_genres_to_postgres',
    python_callable=load_genres_to_postgres,
   
    dag=dag,
)


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


extract_movie_task
extract_genres_task


extract_movie_task >> transform_task

extract_genres_task >> load_genres_task


[transform_task, load_genres_task] >> load_movies_and_genres_task
