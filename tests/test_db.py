import pandas as pd
from sqlalchemy import text
from common.db import get_pg_engine

engine = get_pg_engine()

with engine.connect() as conn:
    df = pd.read_sql(text("SELECT * FROM ratings LIMIT 5"), conn)

print(df)
