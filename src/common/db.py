from sqlalchemy import create_engine
import os


def get_pg_engine():
    """
    Tạo SQLAlchemy engine kết nối PostgreSQL
    Dùng chung cho toàn project
    """

    user = os.getenv("DB_USER", "movie_recommendation_system_seei_user")
    password = os.getenv("DB_PASSWORD", "FassQu6urZDRnOaeNKAxO5XcwCX19Ct0")
    host = os.getenv("DB_HOST", "dpg-d4m61da4d50c73eeecd0-a.oregon-postgres.render.com")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "movie_recommendation_system_seei")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
    )
