# Databricks notebook source
# MAGIC %md
# MAGIC # LoadInto_SQLDatabase

# COMMAND ----------

# MAGIC %run ./SQL_Utils
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load the Gold Delta tables to write

# COMMAND ----------

CATALOG = "sdotc_dev_dbw"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# COMMAND ----------

dim_customer = spark.table(f"{GOLD}.dim_customer")
dim_material = spark.table(f"{GOLD}.dim_material")
dim_currency_rate = spark.table(f"{GOLD}.dim_currency_rate")
dim_sales_organization = spark.table(f"{GOLD}.dim_sales_organization")
dim_date = spark.table(f"{GOLD}.dim_date")
 
fact_sales_order_gold = spark.table(f"{GOLD}.fact_sales_order_gold")
fact_billing_gold = spark.table(f"{GOLD}.fact_billing_gold")
fact_delivery_gold = spark.table(f"{GOLD}.fact_delivery_gold")
fact_order_to_cash_gold = spark.table(f"{GOLD}.fact_order_to_cash_gold")

# COMMAND ----------

#dim_currency_rate.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Dimensions: load first, in parallel

# COMMAND ----------

dim_tables = {
    "dim_customer": dim_customer,
    "dim_material": dim_material,
    "dim_currency_rate": dim_currency_rate,
    "dim_sales_organization": dim_sales_organization,
    "dim_date": dim_date,
}
 
dim_results = load_tables_parallel(dim_tables, max_workers=4, mode="overwrite") #mode= "append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Stop here if any dimension failed

# COMMAND ----------

failed_dims = [name for name, status in dim_results.items() if status != "success"]
if failed_dims:
    raise Exception(f"Dimension load failed for: {failed_dims}. Fix before loading facts.")
 
print("All dimensions loaded successfully — proceeding to facts.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Facts: repartition larger tables, then load in parallel

# COMMAND ----------

fact_tables = {
    "fact_sales_order_gold": fact_sales_order_gold.repartition(8),
    "fact_billing_gold": fact_billing_gold.repartition(8),
    "fact_delivery_gold": fact_delivery_gold.repartition(8),
    "fact_order_to_cash_gold": fact_order_to_cash_gold.repartition(8),
}
 
fact_results = load_tables_parallel(fact_tables, max_workers=4, mode="overwrite") # mode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Summary

# COMMAND ----------

all_results = {**dim_results, **fact_results}
for name, status in all_results.items():
    print(f"{name}: {status}")
 
failed = [name for name, status in all_results.items() if status != "success"]
if failed:
    raise Exception(f"Load completed with failures: {failed}")
else:
    print("\nAll tables loaded successfully.")