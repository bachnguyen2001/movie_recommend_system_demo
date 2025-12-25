import pandas as pd
from sqlalchemy import text

from common.db import get_pg_engine
from serving.routing import get_user_stage


def get_recommendations_from_db(user_id: int, model_version: str, top_k: int = 10) -> pd.DataFrame:
    """
    Fetch pre-computed recommendations from Postgres
    """
    engine = get_pg_engine()
    
    query = """
        SELECT movie_id, score, model_version
        FROM recommendations
        WHERE user_id = :user_id 
          AND model_version = :model_version
        ORDER BY score DESC
        LIMIT :top_k
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(
            text(query), 
            conn, 
            params={
                "user_id": user_id, 
                "model_version": model_version, 
                "top_k": top_k
            }
        )
    
    return df


def recommend_for_user(user_id: int, top_k: int = 10) -> pd.DataFrame:
    """
    Unified recommendation entrypoint (Offline Batch Serving)
    """

    stage = get_user_stage(user_id)
    
    # 1. Decide which model version to use
    if stage == "cold":
        # Popularity (fallback for new users)
        # Note: Popularity recommendations usually have a special user_id (e.g. -1) OR 
        # we just query top items without user_id filter if the table structure supports it.
        # But based on typical design, let's assume we query specific user or global
        # For simplicity in this project's context where we fill recommendations table:
        model_version = "popularity_v1"
        # Strategy: Logic might need to be "get global top" if user not in DB.
        # But let's try to fetch for this user first or fallback.
        # Actually popularity script writes with user_id = -1 or user_id = actual?
        # Let's check popularity script again. It uses user_id input or -1.
        # To be safe, let's fetch global popularity if user specific not found.
        # But for now, let's map stage to model_version.
        target_version = "popularity_v1"
        
    elif stage == "warm":
        target_version = "itemcf_v1"
        
    else: # hot
        target_version = "als_v1"

    # 2. Try to fetch from DB
    df = get_recommendations_from_db(user_id, target_version, top_k)
    
    # 3. Fallback logic (if Airflow hasn't run for this user yet)
    if df.empty:
        # Fallback to global popularity
        df = get_recommendations_from_db(-1, "popularity_v1", top_k)
        
    return df
