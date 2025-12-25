import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from ingestion.filter_ratings import filter_ratings


# ================= CONFIG =================
TOP_K = 10
MODEL_VERSION = "itemcf_v1"
SIM_PATH = Path("data-lake/models/itemcf/v1/item_similarity.parquet")
# =========================================


def load_item_similarity() -> pd.DataFrame:
    if not SIM_PATH.exists():
        raise FileNotFoundError(f"Missing ItemCF artifact: {SIM_PATH}")
    return pd.read_parquet(SIM_PATH)


def generate_itemcf_recommendations() -> pd.DataFrame:
    """
    Generate personalized recommendations using ItemCF artifact
    """

    ratings = filter_ratings()
    sim_df = load_item_similarity()

    # Build quick lookup: item -> [(neighbor, score)]
    sim_lookup = defaultdict(list)
    for row in sim_df.itertuples(index=False):
        sim_lookup[row.item_id].append(
            (row.neighbor_item_id, row.similarity_score)
        )

    user_groups = ratings.groupby("userId")["movieId"].apply(set)

    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    records = []

    for user_id, seen_items in user_groups.items():
        scores = defaultdict(float)

        
        # Sum of ratings for items the user has seen, used for normalization
        # This is the denominator for the weighted average
        rating_sum = sum(seen_items_with_ratings.values())

        for item_id, rating in seen_items_with_ratings.items():
            for neighbor, sim in sim_lookup.get(item_id, []):
                if neighbor in seen_items:
                    continue
                # Accumulate weighted similarity (similarity * user's rating for the item)
                scores[neighbor] += sim * rating

        if not scores:
            continue

        for m in scores:
            # Divide by the sum of ratings to get a weighted average similarity
            if rating_sum > 0: # Avoid division by zero if a user has no ratings (shouldn't happen with filter_ratings)
                scores[m] /= rating_sum
            scores[m] /= 5.0  # Normalize to 0-1 (assuming max rating is 5)

        top_items = sorted(
            scores.items(),
            key=lambda x: x[1],
            reverse=True
        )[:TOP_K]

        for movie_id, score in top_items:
            records.append({
                "user_id": int(user_id),
                "movie_id": int(movie_id),
                "score": float(score),
                "model_version": MODEL_VERSION,
                "generated_at": now,
            })

    return pd.DataFrame(records)
