from ingestion.load_ratings import load_ratings


def run_eda():
    df = load_ratings()

    num_users = df["userId"].nunique()
    num_movies = df["movieId"].nunique()
    num_ratings = len(df)

    ratings_per_user = df.groupby("userId")["movieId"].count()
    ratings_per_movie = df.groupby("movieId")["userId"].count()

    print("===== BASIC STATS =====")
    print(f"Users        : {num_users}")
    print(f"Movies       : {num_movies}")
    print(f"Ratings      : {num_ratings}")
    print(f"Avg/User     : {ratings_per_user.mean():.2f}")
    print(f"Avg/Movie    : {ratings_per_movie.mean():.2f}")

    sparsity = 1 - (num_ratings / (num_users * num_movies))
    print(f"Sparsity     : {sparsity:.4f}")


if __name__ == "__main__":
    run_eda()
