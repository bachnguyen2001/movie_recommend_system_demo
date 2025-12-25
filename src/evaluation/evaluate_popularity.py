from ingestion.filter_ratings import filter_ratings
from evaluation.split import leave_one_out_split
from evaluation.metrics import precision_at_k, recall_at_k, hit_rate_at_k
from serving.popularity_inference import get_popularity_recommendations


def evaluate_popularity(k=10):
    ratings = filter_ratings()
    _, test_df = leave_one_out_split(ratings)

    test_items = test_df.groupby("userId")["movieId"].apply(list)

    # PRECOMPUTE ONCE
    recs = (
        ratings.groupby("movieId")
        .size()
        .sort_values(ascending=False)
        .head(k)
        .index
        .tolist()
    )

    precisions, recalls, hits = [], [], []

    for gt_items in test_items:
        precisions.append(precision_at_k(recs, gt_items, k))
        recalls.append(recall_at_k(recs, gt_items, k))
        hits.append(hit_rate_at_k(recs, gt_items, k))

    return {
        "precision@k": sum(precisions) / len(precisions),
        "recall@k": sum(recalls) / len(recalls),
        "hit_rate@k": sum(hits) / len(hits),
    }



if __name__ == "__main__":
    metrics = evaluate_popularity(k=10)
    print(metrics)
