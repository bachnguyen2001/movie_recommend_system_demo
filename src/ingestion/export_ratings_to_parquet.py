import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from datetime import date

# ===== CONFIG =====
DB_URI = "postgresql://movie_recommendation_system_seei_user:FassQu6urZDRnOaeNKAxO5XcwCX19Ct0@dpg-d4m61da4d50c73eeecd0-a.oregon-postgres.render.com/movie_recommendation_system_seei"
EXPORT_BASE = Path("data-lake/raw/ratings")
DT = date.today().isoformat()
# ==================


def export_ratings():
    engine = create_engine(DB_URI)

    query = """
        SELECT
            "userId",
            "movieId",
            rating,
            timestamp
        FROM ratings
    """

    print("[INGEST] Reading ratings from Postgres...")
    df = pd.read_sql(query, engine)
    print(f"[INGEST] Loaded {len(df):,} rows")

    out_dir = EXPORT_BASE / f"dt={DT}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    print(f"[INGEST] Saved parquet → {out_path}")


if __name__ == "__main__":
    export_ratings()
