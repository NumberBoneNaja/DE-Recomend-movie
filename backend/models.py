from typing import Optional, List
from sqlmodel import Field, Relationship, SQLModel
from datetime import date

class MovieGenre(SQLModel, table=True):
    __tablename__ = "movie_genres"

    movie_id: int = Field(foreign_key="movies.id", primary_key=True)
    genre_id: int = Field(foreign_key="genres.id", primary_key=True)

class Genre(SQLModel, table=True):
    __tablename__ = "genres"

    id: int = Field(primary_key=True)
    name: str

    movies: List["Movie"] = Relationship(back_populates="genres", link_model=MovieGenre)

class Movie(SQLModel, table=True):
    __tablename__ = "movies"

    id: int = Field(primary_key=True, index=True)
    title: str
    overview: Optional[str] = None
    release_date: Optional[date] = None
    original_language: Optional[str] = None
    popularity: Optional[float] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    adult: Optional[bool] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None

    genres: List[Genre] = Relationship(back_populates="movies", link_model=MovieGenre)
