from sqlalchemy import create_engine
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
import joblib

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "recommend_db"

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ===== Load data from PostgreSQL =====
engine = create_engine(DATABASE_URL)



query = """
SELECT 
    m.id,
    m.title,
    m.overview,
    m.release_date,
    m.original_language,
    m.popularity,
    m.vote_average,
    m.vote_count,
    m.adult,
    STRING_AGG(g.name, ', ') AS genre
FROM movies m
LEFT JOIN movie_genres mg ON m.id = mg.movie_id
LEFT JOIN genres g ON mg.genre_id = g.id
GROUP BY m.id ;
"""

df = pd.read_sql(query, engine)
df_new = df
feature =["id","title","overview","release_date","original_language","popularity","vote_average","vote_count","adult","genre"]
df_new = df_new[feature]
df_new["release_date"] = df_new["release_date"].astype(str)
df_new["combind_feature"] = df_new["title"] + " " + df_new["overview"] + " " + df_new["original_language"] + " " + df_new["popularity"].astype(str) + " " + df_new["vote_average"].astype(str) + " " + df_new["vote_count"].astype(str) + " " + df_new["adult"].astype(str) + " " + df_new["genre"]

# Fill NaN values with empty strings
df_new["combind_feature"] = df_new["combind_feature"].fillna("")

cv = CountVectorizer()
count_matrix = cv.fit_transform(df_new["combind_feature"])
cosine_sim = cosine_similarity(count_matrix)

userInput = 'Jurassic World: Fallen Kingdom'
ref_index = df_new[df_new.title.str.contains(userInput,case=False)].index[0]
similar_movies = list(enumerate(cosine_sim[ref_index]))
sorted_similar_movies = sorted(similar_movies,key=lambda x:x[1],reverse=True)[:]

print("Recommend movie for [' + userInput + ']")
print("data.shpe:",df_new.shape)
print('---------------------------------------')
for i,element in enumerate(sorted_similar_movies):
  similar_movie_id = element[0]
  similar_movie_title = df_new.iloc[similar_movie_id].title
  s_score = element[1]
  print(i+1,similar_movie_title,s_score)
  if i > 5 :
    break
  

# df_new[['id','title']].to_pickle("model/movie_data.pkl")
joblib.dump(cv, "model/count_vectorizer.pkl")
joblib.dump(cosine_sim, "model/cosine_sim.pkl")
df_new.to_pickle("model/movie_data.pkl")