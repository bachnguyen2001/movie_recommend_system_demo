from src.serving.recommend_service import recommend_for_user

df = recommend_for_user(user_id=1, top_k=5)
print(df)

df = recommend_for_user(user_id=999999, top_k=5)
print(df)
