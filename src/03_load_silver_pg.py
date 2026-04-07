"""Load silver parquet into PostgreSQL silver schema for DBT consumption."""
import pandas as pd
from sqlalchemy import create_engine, text

from config import DATABASE_URL, SILVER_DIR

SILVER_FILE_PATH = SILVER_DIR / "online_retail_ii_silver.parquet"


def main() -> None:
    engine = create_engine(DATABASE_URL)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(
            text("DROP TABLE IF EXISTS silver.online_retail_ii CASCADE")
        )

    df = pd.read_parquet(SILVER_FILE_PATH)

    df.to_sql(
        "online_retail_ii",
        engine,
        schema="silver",
        if_exists="append",
        index=False,
        chunksize=10_000,
    )

    print(f"Silver data loaded to PostgreSQL silver schema: {len(df)} rows")
    print(f"Table: silver.online_retail_ii")


if __name__ == "__main__":
    main()
