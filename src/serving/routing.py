from typing import Literal
import pandas as pd

from ingestion.filter_ratings import filter_ratings


UserStage = Literal["cold", "warm"]


def get_user_interaction_count() -> pd.Series:
    """
    Return:
        user_id -> interaction_count
    """
    ratings = filter_ratings()
    return ratings.groupby("userId").size()


def get_user_stage(user_id: int, min_interactions: int = 10) -> UserStage:
    """
    Decide which model to use for a user
    """
    user_counts = get_user_interaction_count()

    count = user_counts.get(user_id, 0)

    if count < 10:
        return "cold"      # popularity
    elif count < 30:
        return "warm"      # itemcf
    else:
        return "hot"       # ALS

    return "warm"
