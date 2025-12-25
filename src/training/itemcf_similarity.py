import pandas as pd
from collections import defaultdict
from math import sqrt
from pathlib import Path
from itertools import combinations
from datetime import datetime

from src.ingestion.filter_ratings import filter_ratings


# ================= CONFIG =================
MAX_ITEMS_PER_USER = 50
MIN_ITEM_FREQ = 20
MIN_COOC_COUNT = 3
MIN_SIMILARITY = 0.05
TOP_M_NEIGHBORS = 50

MODEL_VERSION = "itemcf_v1"

ARTIFACT_DIR = Path("data-lake/models/itemcf/v1")
ARTIFACT_PATH = ARTIFACT_DIR / "item_similarity.parquet"    
# =========================================


def build_item_cooccurrence(df: pd.DataFrame):
    """
    Build item–item co-occurrence matrix (pruned)
    """

    # 1️⃣ Prune rare items
    item_freq = df["movieId"].value_counts()
    valid_items = set(item_freq[item_freq >= MIN_ITEM_FREQ].index)
    df = df[df["movieId"].isin(valid_items)]

    # 2️⃣ Group by user
    user_groups = df.groupby("userId")["movieId"].apply(list)

    cooccurrence = defaultdict(int)
    item_count = defaultdict(int)

    for idx, items in enumerate(user_groups):
        if idx % 1000 == 0:
            print(f"[ItemCF] Processed {idx} users")

        unique_items = list(set(items))[:MAX_ITEMS_PER_USER]

        # Count item frequency
        for i in unique_items:
            item_count[i] += 1

        # Count co-occurrence
        for i, j in combinations(unique_items, 2):
            cooccurrence[(i, j)] += 1
            cooccurrence[(j, i)] += 1

    # Prune weak co-occurrence early
    cooccurrence = {
        (i, j): c
        for (i, j), c in cooccurrence.items()
        if c >= MIN_COOC_COUNT
    }

    return cooccurrence, item_count


def compute_item_similarity(cooccurrence, item_count) -> pd.DataFrame:
    """
    Compute cosine similarity + keep top-M neighbors
    """

    neighbors = defaultdict(list)

    for (i, j), cij in cooccurrence.items():
        sim = cij / sqrt(item_count[i] * item_count[j])
        if sim >= MIN_SIMILARITY:
            neighbors[i].append((j, sim))

    now = datetime.utcnow()
    records = []

    for item_i, sims in neighbors.items():
        top_neighbors = sorted(
            sims, key=lambda x: x[1], reverse=True
        )[:TOP_M_NEIGHBORS]

        for item_j, score in top_neighbors:
            records.append({
                "item_id": int(item_i),
                "neighbor_item_id": int(item_j),
                "similarity_score": float(score),
                "model_version": MODEL_VERSION,
                "generated_at": now,
            })

    return pd.DataFrame(records)


def train_itemcf_similarity() -> pd.DataFrame:
    """
    Full ItemCF pipeline
    """
    df = filter_ratings()
    cooccurrence, item_count = build_item_cooccurrence(df)
    sim_df = compute_item_similarity(cooccurrence, item_count)
    return sim_df


def save_similarity(sim_df: pd.DataFrame):
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    sim_df.to_parquet(ARTIFACT_PATH, index=False)
    print(f"[OK] Saved ItemCF similarity → {ARTIFACT_PATH}")


if __name__ == "__main__":
    sim_df = train_itemcf_similarity()
    print(sim_df.head())
    print("Total similarity pairs:", len(sim_df))
    save_similarity(sim_df)
