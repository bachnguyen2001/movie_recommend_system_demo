import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime

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

    now = datetime.utcnow()
    records = []

    for user_id, seen_items in user_groups.items():
        scores = defaultdict(float)

        for item in seen_items:
            for neighbor, sim in sim_lookup.get(item, []):
                if neighbor in seen_items:
                    continue
                scores[neighbor] += sim

        if not scores:
            continue

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
