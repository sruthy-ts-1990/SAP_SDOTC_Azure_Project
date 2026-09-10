# Databricks notebook source
# MAGIC %md
# MAGIC #Common_SQL_Utils

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection settings

# COMMAND ----------

SQL_SERVER = "sdotc-dev-sql.database.windows.net"
SQL_DATABASE = "sdotc-dev-sqldb"
  
_common_opts = {
    "host": SQL_SERVER,
    "port": "1433",
    "database": SQL_DATABASE,
    "user": "sqladmin",
    "encrypt": "true",
    "trustServerCertificate": "false",
    # Bulk-copy tuning — this is what actually cuts load time down,
    # not just the code de-duplication:
    "tableLock": "true",
    "batchsize": "100000",
    "reliabilityLevel": "BEST_EFFORT",
}
 
print("Azure SQL connection configuration loaded successfully.")
print(f"Server   : {SQL_SERVER}")
print(f"Database : {SQL_DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable writer with retry

# COMMAND ----------

def _write_sqlserver(df, dbtable, mode="append", max_retries=3, retry_delay=30):
    for attempt in range(1, max_retries + 1):
        try:
            writer = df.write.format("sqlserver").mode(mode)
            for k, v in _common_opts.items():
                writer = writer.option(k, v)
            writer.option("dbtable", dbtable).save()
            return
        except Exception as e:
            if attempt < max_retries and "not currently available" in str(e):
                print(f"[Attempt {attempt}/{max_retries}] SQL Server temporarily unavailable, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Parallel multi-table loader

# COMMAND ----------

def load_tables_parallel(tables_to_load: dict, max_workers=4, mode="append"):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_write_sqlserver, df, name, mode): name
            for name, df in tables_to_load.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
                print(f"✓ {name} loaded")
                results[name] = "success"
            except Exception as e:
                print(f"✗ {name} failed: {e}")
                results[name] = f"failed: {e}"
    return results