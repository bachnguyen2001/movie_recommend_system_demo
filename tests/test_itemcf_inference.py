from src.serving.itemcf_inference import generate_itemcf_recommendations

df = generate_itemcf_recommendations()
print(df.head())
print("Total recommendations:", len(df))
