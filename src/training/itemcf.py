import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime

from ingestion.filter_ratings import filter_ratings


# ================= CONFIG =================
MODEL_VERSION = "itemcf_v1"
TOP_K = 10
# =========================================


def train_item_similarity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build item-item cosine similarity matrix
    item-user matrix: rows=item, cols=user
    """

    user_item = df.pivot_table(
        index="movie_id",
        columns="user_id",
        values="rating",
        fill_value=0.0,
    )

    similarity = cosine_similarity(user_item.values)

    sim_df = pd.DataFrame(
        similarity,
        index=user_item.index,
        columns=user_item.index,
    )

    return sim_df


def recommend_for_user(
    user_id: int,
    sim_df: pd.DataFrame,
    df: pd.DataFrame,
    top_k: int = 10,
) -> pd.DataFrame:
    """
    Weighted ItemCF recommendation for one user
    """

    user_hist = (
        df[df["user_id"] == user_id]
        .set_index("movie_id")["rating"]
    )

    if user_hist.empty:
        return pd.DataFrame(columns=["movie_id", "score"])

    scores = {}
    rating_sum = user_hist.sum()

    for movie_i, rating_i in user_hist.items():
        if movie_i not in sim_df:
            continue

        # similarity vector of movie_i to all movies
        sim_series = sim_df[movie_i]

        for movie_j, sim_ij in sim_series.items():
            if movie_j in user_hist:
                continue

            scores[movie_j] = scores.get(movie_j, 0.0) + sim_ij * rating_i

    if not scores:
        return pd.DataFrame(columns=["movie_id", "score"])

    # normalize
    for m in scores:
        scores[m] /= rating_sum

    recs = (
        pd.Series(scores)
        .sort_values(ascending=False)
        .head(top_k)
        .reset_index()
    )
    recs.columns = ["movie_id", "score"]

    return recs


def generate_itemcf_recommendations(top_k: int = 10) -> pd.DataFrame:
    """
    Generate recommendations for all users
    """

    df = filter_ratings()
    sim_df = train_item_similarity(df)

    users = df["user_id"].unique()
    now = datetime.utcnow()

    rows = []

    for user_id in users:
        user_recs = recommend_for_user(
            user_id=user_id,
            sim_df=sim_df,
            df=df,
            top_k=top_k,
        )

        for _, row in user_recs.iterrows():
            rows.append(
                {
                    "user_id": int(user_id),
                    "movie_id": int(row["movie_id"]),
                    "score": float(row["score"]),
                    "model_version": MODEL_VERSION,
                    "generated_at": now,
                }
            )

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df_recs = generate_itemcf_recommendations(top_k=TOP_K)
    print(df_recs.head())
    print("Total recommendations:", len(df_recs))
