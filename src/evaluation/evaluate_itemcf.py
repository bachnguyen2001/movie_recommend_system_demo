from collections import defaultdict
import pandas as pd

from ingestion.filter_ratings import filter_ratings
from evaluation.split import leave_one_out_split
from evaluation.metrics import precision_at_k, recall_at_k, hit_rate_at_k
from serving.itemcf_inference import generate_itemcf_recommendations


def evaluate_itemcf(k=10):
    ratings = filter_ratings()
    train_df, test_df = leave_one_out_split(ratings)

    test_items = test_df.groupby("userId")["movieId"].apply(list)

    rec_df = load_itemcf_recommendations()   # ❗ không train

    user_recs = (
        rec_df
        .sort_values(["user_id", "score"], ascending=[True, False])
        .groupby("user_id")["movie_id"]
        .apply(list)
    )

    precisions, recalls, hits = [], [], []

    for user_id, gt_items in test_items.items():
        recs = user_recs.get(user_id, [])[:k]

        precisions.append(precision_at_k(recs, gt_items, k))
        recalls.append(recall_at_k(recs, gt_items, k))
        hits.append(hit_rate_at_k(recs, gt_items, k))

    return {
        "precision@k": sum(precisions) / len(precisions),
        "recall@k": sum(recalls) / len(recalls),
        "hit_rate@k": sum(hits) / len(hits),
    }


if __name__ == "__main__":
    metrics = evaluate_itemcf(k=10)
    print(metrics)
