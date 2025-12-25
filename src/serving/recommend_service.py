import pandas as pd

from serving.routing import get_user_stage
from serving.popularity_inference import get_popularity_recommendations
from serving.itemcf_inference import generate_itemcf_recommendations


def recommend_for_user(user_id: int, top_k: int = 10) -> pd.DataFrame:
    """
    Unified recommendation entrypoint
    """

    stage = get_user_stage(user_id)

    if stage == "cold":
        return get_popularity_recommendations(top_k=top_k, user_id=user_id)

    # warm user
    df = generate_itemcf_recommendations()
    return df[df["user_id"] == user_id].head(top_k)
