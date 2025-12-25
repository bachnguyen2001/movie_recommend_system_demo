import pandas as pd
from sqlalchemy import text

from common.db import get_pg_engine


def write_recommendations(df: pd.DataFrame, model_version: str):
    """
    Ghi recommendations vào bảng recommendations
    - Xóa dữ liệu cũ theo model_version
    - Insert dữ liệu mới
    """

    engine = get_pg_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM recommendations
                WHERE model_version = :model_version
                """
            ),
            {"model_version": model_version},
        )

        df.to_sql(
            "recommendations",
            con=conn,
            if_exists="append",
            index=False,
        )
