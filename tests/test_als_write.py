from src.serving.als_inference import generate_als_recommendations_pandas
from src.serving.write_recommendations import write_recommendations

# safety first: limit users on first run
df = generate_als_recommendations_pandas(limit_users=5000)
write_recommendations(df, model_version="als_v1")

print("Wrote ALS recommendations:", len(df))

