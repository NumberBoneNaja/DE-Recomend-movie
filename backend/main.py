from fastapi import FastAPI, HTTPException, Depends, Query
from typing import Annotated
from sqlmodel import SQLModel, Session, create_engine, select
import joblib
import pandas as pd
from models import Movie  # import จากไฟล์ models.py
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()

# โหลด model และ data
cv = joblib.load("model/count_vectorizer.pkl")
cosine_sim = joblib.load("model/cosine_sim.pkl")
df_new = pd.read_pickle("model/movie_data.pkl")

# DB Config
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "recommend_db"

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL, echo=True)

# สร้างตาราง (ครั้งแรกเท่านั้น)
# SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]

app.add_middleware(
    CORSMiddleware,
    allow_origins='*',
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Movie recommendation API is running"}

@app.get("/recommend/")
def recommend_movies(title: str):
    matched_movies = df_new[df_new.title.str.contains(title, case=False)]
    if matched_movies.empty:
        raise HTTPException(status_code=404, detail="Movie not found")
    
    ref_index = matched_movies.index[0]
    similar_movies = list(enumerate(cosine_sim[ref_index]))
    sorted_similar_movies = sorted(similar_movies, key=lambda x: x[1], reverse=True)[0:]
    
    recommendations = []
    for i, element in enumerate(sorted_similar_movies):
        similar_movie_id = element[0]
        recommendations.append({
            "id": int(df_new.iloc[similar_movie_id]['id']),
            "title": df_new.iloc[similar_movie_id]['title'], 
            "score": element[1]})
        if i >= 5:
            break
    
    return {"query": title, "recommendations": recommendations}



@app.get("/movies/")
def read_movies(
    session: SessionDep,
    offset: int = 0,
    limit: Annotated[int, Query(le=100)] = 100,
) -> list[Movie]:
    movies = session.exec(select(Movie).offset(offset).limit(limit)).all()
    return movies

@app.get("/movies/{movie_id}")
def read_movie(
    movie_id: int,
    session: SessionDep,
) -> Movie:
    movie = session.get(Movie, movie_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    return movie

