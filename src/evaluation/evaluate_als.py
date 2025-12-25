from collections import defaultdict
from src.ingestion.filter_ratings import filter_ratings
from src.evaluation.split import leave_one_out_split
from src.evaluation.metrics import precision_at_k, recall_at_k, hit_rate_at_k
from src.serving.als_inference import generate_als_recommendations_pandas


def evaluate_als(k=10, sample_users=5000):
    ratings = filter_ratings()
    train_df, test_df = leave_one_out_split(ratings)

    test_items = test_df.groupby("userId")["movieId"].apply(list)

    rec_df = generate_als_recommendations_pandas(limit_users=sample_users)
    user_recs = rec_df.groupby("user_id")["movie_id"].apply(list)

    precisions, recalls, hits = [], [], []

    for user_id, gt_items in test_items.items():
        if user_id not in user_recs:
            continue
        recs = user_recs[user_id]

        precisions.append(precision_at_k(recs, gt_items, k))
        recalls.append(recall_at_k(recs, gt_items, k))
        hits.append(hit_rate_at_k(recs, gt_items, k))

    n = max(1, len(precisions))
    return {
        "precision@k": sum(precisions) / n,
        "recall@k": sum(recalls) / n,
        "hit_rate@k": sum(hits) / n,
        "users_evaluated": n,
    }


if __name__ == "__main__":
    print(evaluate_als(k=10, sample_users=5000))
