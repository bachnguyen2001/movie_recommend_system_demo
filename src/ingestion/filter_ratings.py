from ingestion.load_ratings import load_ratings
from functools import lru_cache

@lru_cache(maxsize=1)
def filter_ratings(min_user_ratings=3, min_movie_ratings=5):
    df = load_ratings()

    user_cnt = df.groupby("userId")["movieId"].count()
    valid_users = user_cnt[user_cnt >= min_user_ratings].index

    movie_cnt = df.groupby("movieId")["userId"].count()
    valid_movies = movie_cnt[movie_cnt >= min_movie_ratings].index

    df_filtered = df[
        df["userId"].isin(valid_users)
        & df["movieId"].isin(valid_movies)
    ]

    print("Before filter:", len(df))
    print("After filter :", len(df_filtered))

    return df_filtered
