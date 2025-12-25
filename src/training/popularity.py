import pandas as pd
from datetime import datetime

from ingestion.filter_ratings import filter_ratings


def train_popularity(top_k: int = 10) -> pd.DataFrame:
    """
    Train popularity model:
    - Đếm số lần movie được rating
    - Lấy Top-K movie phổ biến nhất
    """

    df = filter_ratings()

    popularity = (
        df.groupby("movieId")["userId"]
        .count()
        .reset_index(name="cnt")
        .sort_values("cnt", ascending=False)
        .head(top_k)
    )

    max_cnt = popularity["cnt"].max()
    popularity["score"] = popularity["cnt"] / max_cnt
    popularity = popularity[["movieId", "score"]]

    return popularity


def generate_recommendations_for_all_users(top_k: int = 10) -> pd.DataFrame:
    """
    Generate recommendation cho TẤT CẢ user
    (vì popularity là global)
    """

    df = filter_ratings()
    users = df["userId"].unique()

    top_movies = train_popularity(top_k)

    recs = []
    now = datetime.utcnow()

    for user_id in users:
        for _, row in top_movies.iterrows():
            recs.append(
                {
                    "user_id": int(user_id),
                    "movie_id": int(row["movieId"]),
                    "score": float(row["score"]),
                    "model_version": "popularity_global_v1",
                    "generated_at": now,
                }
            )

    return pd.DataFrame(recs)


if __name__ == "__main__":
    df_recs = generate_recommendations_for_all_users(top_k=10)
    print(df_recs.head())
    print("Total recommendations:", len(df_recs))
