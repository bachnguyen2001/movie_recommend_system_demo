from ingestion.load_ratings import load_ratings

df = load_ratings()
print(df.head())
print("Total rows:", len(df))
