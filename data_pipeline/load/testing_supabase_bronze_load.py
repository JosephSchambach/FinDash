import os
import pandas as pd
from supabase import create_client, Client

from dotenv import load_dotenv
load_dotenv()

# --- ENV VARS ---
SUPABASE_URL = os.environ.get("SUPABASE_STORAGE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")  # needs service role key, not anon
STORAGE_BUCKET = "raw-stock-data"
BRONZE_TABLE = "stock_data"
BRONZE_SCHEMA = "bronze"

# --- INIT CLIENT ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- DOWNLOAD FILES ---
def download_parquet_files(bucket: str, prefix: str = "") -> list[str]:
    files = supabase.storage.from_(bucket).list(path="stock_prices")
    local_files = []

    for f in files:
        name = f["name"]
        # Only handle parquet
        if not name.endswith(".parquet"):
            continue

        data = supabase.storage.from_(bucket).download(f"stock_prices/{name}")
        local_path = os.path.join("tmp", name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, "wb") as f_out:
            f_out.write(data)
        local_files.append(local_path)

    return local_files


# --- UPSERT INTO BRONZE ---
def upsert_parquet_to_table(file_path: str, schema: str, table_name: str):
    df = pd.read_parquet(file_path)

    # Convert dataframe to list of dicts for Supabase insert
    rows = df.to_dict(orient="records")

    # Convert any 'timestamp' field from pd.Timestamp to ISO string
    for row in rows:
        if "timestamp" in row and isinstance(row["timestamp"], pd.Timestamp):
            row["timestamp"] = row["timestamp"].isoformat()
    # Upsert (insert or update if conflict on PK)
    resp = supabase.table(f"{schema}.{table_name}").upsert(rows).execute()
    print(f"Upserted {len(rows)} rows from {file_path}: {resp}")


if __name__ == "__main__":
    parquet_files = download_parquet_files(STORAGE_BUCKET)

    for pf in parquet_files:
        upsert_parquet_to_table(pf, BRONZE_SCHEMA, BRONZE_TABLE)
