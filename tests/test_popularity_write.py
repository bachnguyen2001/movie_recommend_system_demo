from training.popularity import generate_recommendations_for_all_users
from serving.write_recommendations import write_recommendations

df = generate_recommendations_for_all_users(top_k=10)
write_recommendations(df, model_version="popularity_global_v1")

print("Wrote recommendations:", len(df))
