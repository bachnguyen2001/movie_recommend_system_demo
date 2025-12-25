import pandas as pd
from datetime import datetime

from ingestion.filter_ratings import filter_ratings


MODEL_VERSION = "popularity_v1"


def get_popularity_recommendations(
    top_k: int = 10,
    user_id: int | None = None,
) -> pd.DataFrame:
    """
    Global popularity recommender.
    Same recommendations for all cold users.
    """

    ratings = filter_ratings()

    # Popularity = number of interactions
    popular_items = (
        ratings.groupby("movieId")
        .size()
        .reset_index(name="score")
        .sort_values("score", ascending=False)
        .head(top_k)
    )

    now = datetime.utcnow()

    popular_items["user_id"] = user_id if user_id is not None else -1
    popular_items["movie_id"] = popular_items["movieId"]
    popular_items["model_version"] = MODEL_VERSION
    popular_items["generated_at"] = now

    return popular_items[
        ["user_id", "movie_id", "score", "model_version", "generated_at"]
    ]
