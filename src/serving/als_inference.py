from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, current_timestamp
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.ml.recommendation import ALSModel

MODEL_PATH = "data-lake/models/als/v1"
TOP_K = 10
MODEL_VERSION = "als_v1"


def get_spark():
    return SparkSession.builder.appName("ALS-Inference").getOrCreate()


def generate_als_recommendations_spark():
    spark = get_spark()
    model = ALSModel.load(MODEL_PATH)

    recs = model.recommendForAllUsers(TOP_K)

    recs = (
        recs.select(col("userId"), explode("recommendations").alias("rec"))
            .select(
                col("userId").alias("user_id"),
                col("rec.movieId").alias("movie_id"),
                col("rec.movieId").alias("movie_id"),
                (col("rec.rating") / 5.0).alias("score"),
            )
            .withColumn("model_version", col("user_id") * 0 + MODEL_VERSION)
            .withColumn("generated_at", current_timestamp())
    )
    return recs


def generate_als_recommendations_pandas(limit_users: int | None = None):
    """
    Convert Spark DF → Pandas (optionally limit users for safety)
    """
    sdf = generate_als_recommendations_spark()
    if limit_users:
        sdf = sdf.limit(limit_users * TOP_K)
    
    pdf = sdf.toPandas()
    pdf["generated_at"] = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    return pdf
