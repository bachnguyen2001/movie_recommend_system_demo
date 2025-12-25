def precision_at_k(recommended, ground_truth, k):
    if not recommended:
        return 0.0
    return len(set(recommended[:k]) & set(ground_truth)) / k


def recall_at_k(recommended, ground_truth, k):
    if not ground_truth:
        return 0.0
    return len(set(recommended[:k]) & set(ground_truth)) / len(ground_truth)


def hit_rate_at_k(recommended, ground_truth, k):
    return int(len(set(recommended[:k]) & set(ground_truth)) > 0)
