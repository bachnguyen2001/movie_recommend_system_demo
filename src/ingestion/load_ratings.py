import pandas as pd
from sqlalchemy import text
from common.db import get_pg_engine


def load_ratings() -> pd.DataFrame:
    """
    Load bảng ratings từ PostgreSQL
    """
    engine = get_pg_engine()

    query = """
        SELECT
            "userId",
            "movieId",
            rating,
            timestamp
        FROM ratings
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df
