CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    overview TEXT,
    release_date DATE,
    original_language VARCHAR(10),
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INTEGER,
    adult BOOLEAN,
    poster_path TEXT,
    backdrop_path TEXT
);
CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name TEXT
);
CREATE TABLE movie_genres (
    movie_id INTEGER REFERENCES movies(id),
    genre_id INTEGER REFERENCES genres(id),
    PRIMARY KEY (movie_id, genre_id)
);
