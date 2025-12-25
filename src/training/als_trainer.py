from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pathlib import Path

MODEL_DIR = Path("data-lake/models/als/v1")
DATA_PATH = "data-lake/raw/ratings"


def get_spark():
    return (
        SparkSession.builder
        .appName("ALS-Training")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def train_als():
    spark = get_spark()

    print("[ALS] Loading ratings parquet...")
    ratings = spark.read.parquet(DATA_PATH)

    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        rank=50,
        maxIter=10,
        regParam=0.1,
        implicitPrefs=False,
        coldStartStrategy="drop",
    )

    print("[ALS] Training model...")
    model = als.fit(ratings)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(MODEL_DIR))

    print(f"[ALS] Model saved → {MODEL_DIR}")


if __name__ == "__main__":
    train_als()
