# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import *
from pyspark.sql.functions import col


# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "manual", "pipeline run ID")
dbutils.widgets.text("job_id", "manual_run")
dbutils.widgets.text("catalog", "sdotc_dev_dbw")

# COMMAND ----------

JOB_ID = dbutils.widgets.get("job_id")
LOAD_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)
NOTEBOOK_NAME= "Silver Transformations"

CATALOG=dbutils.widgets.get("catalog")
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
DEFAULT_SCHEMA = f"{CATALOG}.default"

TOLERANCE_PCT = 0.5                       # DQ breach threshold, tune per team agreement

# COMMAND ----------

df_config = spark.read.table(f"{DEFAULT_SCHEMA}.Table_Columns_Config")
df_hierarchy_config = spark.read.table(f"{DEFAULT_SCHEMA}.Table_Hierarchy_Config")

df_tblNames=df_config.select("table_name").distinct()

dict_hierarchy = {
    r["Header_Table"]: r["Item_Table"]
    for r in (df_hierarchy_config
        .select("Header_Table", "Item_Table")
        .collect()
    )
    }

dfs = {} #holds hierarchial table dfs

# COMMAND ----------

# MAGIC %md
# MAGIC ###HELPERS

# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql import Window, DataFrame
from pyspark.sql import functions as F

def dedup_latest_change_wins(df: DataFrame, key_cols: list, table_name: str) -> DataFrame:
    w = Window.partitionBy(*key_cols).orderBy(F.col("recordstamp").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    duplicates = ranked.filter(F.col("_rn") > 1)#.drop("_rn")
    clean = ranked.filter(F.col("_rn") == 1).drop("_rn")

    total = df.count()
    bad = duplicates.count()
    pct = round(100 * bad / total, 4) if total else 0.0
    
    return clean


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_nullable_fields(df: DataFrame, non_nullable_fields: list, table_name: str) -> DataFrame:
    
    # Drop rows where ANY of the specified fields is null
    cleaned_df = df.dropna(subset=non_nullable_fields)
    # Build null-count expressions for each field in a single pass
    """null_counts_row = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in non_nullable_fields
    ]).collect()[0]

    violations = {
        c: null_counts_row[c]
        for c in non_nullable_fields
        if null_counts_row[c] and null_counts_row[c] > 0
    }

    if violations and raise_on_failure:
        details = ", ".join(f"{col}: {cnt} nulls" for col, cnt in violations.items())
        raise ValueError(f"Non-nullable field violation(s) found -> {details}")"""

    return cleaned_df

# COMMAND ----------

def decode_sap_date(df: DataFrame, col_name: str) -> DataFrame:
    """Decodes a SAP date column — IDEMPOTENT (see above) AND format-safe.

    Real data check (2026-08-06) showed erdat containing values like
    '2022-02-28' — already standard ISO format, NOT the raw 8-digit SAP
    DATS format ('20220228') originally assumed in the casting plan. Same
    lesson as the vbeln zero-padding and gdatu investigations: verify the
    real format, don't assume it from the SAP field name alone.

    Handles both possible formats safely: tries ISO first (the format
    actually observed), falls back to raw SAP YYYYMMDD if that fails.
    try_to_date returns NULL on a non-match instead of raising an error,
    so coalesce picks whichever pattern actually worked.
    """
    current_type = dict(df.dtypes).get(col_name)
    if current_type in ("date", "timestamp"):
        return df   # already decoded in a previous run — nothing to do

    col_str = F.col(col_name).cast("string")
    parsed = F.coalesce(
        F.try_to_date(col_str, "yyyy-MM-dd"),   # observed real format in this dataset
        F.try_to_date(col_str, "yyyyMMdd"),     # SAP raw DATS format, fallback
    )
    return df.withColumn(col_name, parsed)

# COMMAND ----------

def _base_exception_cols(df, rule_name, source_table, parent_table, key_cols,
                          reason_code, reason_description):
    return (
        df.withColumn("exception_id", F.expr("uuid()"))
          .withColumn("rule_name", F.lit(rule_name))
          .withColumn("source_table", F.lit(source_table))
          .withColumn("parent_table", F.lit(parent_table))
          .withColumn("source_key", F.concat_ws("|", *[F.col(k).cast("string") for k in key_cols]))
          .withColumn("reason_code", F.lit(reason_code))
          .withColumn("reason_description", F.lit(reason_description))
          .withColumn("detected_at", F.lit(RUN_TS))
          .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
          .withColumn("load_date", F.lit(LOAD_DATE).cast("date"))
          .select("exception_id", "rule_name", "source_table", "parent_table", "source_key",
                  "reason_code", "reason_description", "detected_at", "pipeline_run_id",
                  *key_cols, "load_date")
    )

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 11
def referential_integrity_check(child_df, parent_df, key_col, child_table, parent_table, extra_cols=None):
    extra_cols = extra_cols or []
    rule_name = f"REF_INTEGRITY_{child_table}_{parent_table}"

    parent_keys = parent_df.select(key_col).dropDuplicates()
    orphans = child_df.join(parent_keys, on=key_col, how="left_anti")
    exceptions = _base_exception_cols(
        orphans, rule_name, child_table, parent_table, [key_col] + extra_cols,
        "ORPHAN_NO_PARENT",
        f"{child_table} row references a {parent_table} key that does not exist in {parent_table}",
    )
    all_exceptions_dfs.append(exceptions)

    total = child_df.count()
    bad = exceptions.count()
    summary = log_summary(rule_name, child_table, parent_table, total, bad)
    
    #saving Orphan cleaned as level1 -start
    #non_orphans = matched = child_df.join(parent_keys, on=key_col, how="left_semi") 
    
    non_orphans = child_df.alias("c").join(orphans.alias("o"),\
        F.col(f"o.{key_col}") == F.col(f"c.{key_col}"), "left_anti")
    #non_orphans.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
        #.saveAsTable(f"{SILVER_SCHEMA}.{child_table}_stg7")
    #saving Orphan cleaned as level1 -end

    print(f"{child_table}->{parent_table}: {bad}/{total} orphans ({summary['orphan_pct']}%)")
    return non_orphans


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 13
def convert_currency(
    parent_df,
    child_df,
    parent_table_name,
    parent_key_col,
    from_currency_col,
    transaction_date_col,
    to_currency,
    tcurr_df,
    tcurf_df,
    child_table_name,
    child_key_cols
):
    # ---------------------------------------------------------
    # 1. Add a unique ID to each transaction
    # ---------------------------------------------------------
    print(f"parent_table_name:{parent_table_name}")
    fact = (
        parent_df
        .withColumn("_currency_row_id", F.monotonically_increasing_id())
        .withColumn(
            "_from_currency",
            F.upper(F.trim(F.col(from_currency_col)))
        )
        .withColumn(
            "_to_currency",
            F.lit(to_currency.upper())
        )
        .withColumn(
            "_transaction_date",
            F.to_date(F.col(transaction_date_col))
        )
    )

    # ---------------------------------------------------------
    # 2. TCURR
    # ---------------------------------------------------------
    tcurr = (
        tcurr_df
        .select(
            F.col("FCURR").alias("rate_from_currency"),
            F.col("TCURR").alias("rate_to_currency"),
            F.col("valid_from_date").alias("rate_date"),
            F.col("UKURS").cast("double").alias("UKURS")
        )
    )

    # ---------------------------------------------------------
    # 3. As-of join
    # ---------------------------------------------------------
    joined = (
        fact
        .join(
            tcurr,
            (
                (fact["_from_currency"] == tcurr["rate_from_currency"])
                &
                (fact["_to_currency"] == tcurr["rate_to_currency"])
                &
                (tcurr["rate_date"] <= fact["_transaction_date"])
            ),
            "left"
        )
    )

    # ---------------------------------------------------------
    # 4. Select latest TCURR rate
    # ---------------------------------------------------------
    rate_window = (
        Window
        .partitionBy("_currency_row_id")
        .orderBy(F.col("rate_date").desc())
    )
    
    ranked = (
        joined
        .withColumn(
            "_rate_rank",
            F.row_number().over(rate_window)
        )
        .filter(F.col("_rate_rank") == 1)
        .drop("_rate_rank")
    )


    # ---------------------------------------------------------
    # 5. TCURF
    # ---------------------------------------------------------
    tcurf = (
        tcurf_df
        .select(
            F.col("FCURR").alias("ratio_from_currency"),
            F.col("TCURR").alias("ratio_to_currency"),
            F.col("FFACT").alias("FFACT"),
            F.col("TFACT").alias("TFACT"),
            F.col("valid_from_date").alias("tcurf_rate_date")
        )
    )

    # ---------------------------------------------------------
    # 6. Apply TCURF ratio
    # ---------------------------------------------------------
    result = (
        ranked
        .join(
            tcurf,
            (
                (ranked["_from_currency"] ==
                 tcurf["ratio_from_currency"])
                &
                (ranked["_to_currency"] ==
                 tcurf["ratio_to_currency"])
            ),
            "left"
        )
        .withColumn(
            "source_currency",
            F.col("_from_currency")
        )
        .withColumn(
            "target_currency",
            F.col("_to_currency")
        )
        .withColumn(
            "exchange_rate",
            F.col("UKURS")
        )
        .withColumn(
            "exchange_rate_date",
            F.col("rate_date")
        )
        .withColumn(
            "conversion_ratio",
            F.when(
                F.col("FFACT").isNotNull() &
                F.col("TFACT").isNotNull(),
                F.col("TFACT") / F.col("FFACT")
            ).otherwise(F.lit(1.0))
        )
        
    )

    # ---------------------------------------------------------
    # 7. Select latest TCURF rate
    # ---------------------------------------------------------
    rate_window_tcurf = (
        Window
        .partitionBy("_currency_row_id")
        .orderBy(F.col("tcurf_rate_date").desc())
    )
    
    result_ranked = (
        result
        .withColumn(
            "_rate_rank",
            F.row_number().over(rate_window_tcurf)
        )
        .filter(F.col("_rate_rank") == 1)
        .drop("_rate_rank")
    )

    dq_exception = result.filter(F.col("exchange_rate").isNull())
    keyRows=dq_exception.select(parent_key_col[0]).distinct()

    parent_exceptions = _base_exception_cols(
        dq_exception, f"CURRENCY_EXCHANGE_{parent_table_name}", parent_table_name, "",
        parent_key_col,
        "CURRENCY_EXCHANGE_RATE_UNAVAILABLE",
        "currency exchange rate not available for the transaction date")
    all_exceptions_dfs.append(parent_exceptions)

    child_rows = (
    child_df.alias("c")
    .join(
        keyRows.alias("k"),
        F.col(f"c.{parent_key_col[0]}") == F.col(f"k.{parent_key_col[0]}"),
        "inner"
    )
    .select("c.*")  
    )

    child_exceptions = _base_exception_cols(
        child_rows, f"CURRENCY_EXCHANGE_{child_table_name}", child_table_name, parent_table_name,
        child_key_cols,
        "CURRENCY_EXCHANGE_RATE_UNAVAILABLE",
        "currency exchange rate not available for the transaction date")
    all_exceptions_dfs.append(child_exceptions)

    log_summary(parent_table_name,parent_table_name, None, parent_exceptions.count(), parent_exceptions.count(),"update")
    log_summary(child_table_name,child_table_name, parent_table_name,child_exceptions.count(),child_exceptions.count(),"update")
   
    valid_transactions = result_ranked.filter(F.col("exchange_rate").isNotNull())
   
    valid_transactions = valid_transactions.drop(
        "_currency_row_id",
        "_from_currency",
        "_to_currency",
        "_transaction_date",
        "tcurf_rate_date"
    )

    valid_child_transactions = (
    child_df.alias("c")
    .join(
        keyRows.alias("k"),
        F.col(f"c.{parent_key_col[0]}") == F.col(f"k.{parent_key_col[0]}"),
        "left_anti"   # keeps only child rows NOT in keyRows
    )
    .select("c.*")
    )
    
    valid_child_transactions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.{child_table_name}_clean1")
    valid_transactions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.{parent_table_name}_clean1")


# COMMAND ----------

# DBTITLE 1,Cell 14
def log_summary(rule_name, source_table, parent_table, total, bad_rows,action="new"):
    if(action == "new"):
        orphan_pct = round(100 * bad_rows / total, 4) if total else 0.0
        row = {
            "run_id": PIPELINE_RUN_ID,
            "rule_name": rule_name,
            "source_table": source_table,
            "parent_table": parent_table or "",
            "total_child_rows": total,
            "orphan_rows": bad_rows,
            "valid_rows": total - bad_rows,
            "orphan_pct": orphan_pct,
            "notebook_name": NOTEBOOK_NAME,
            "job_id": JOB_ID,
            "run_timestamp": RUN_TS,
            "load_date": LOAD_DATE,
            "_is_breach": orphan_pct > TOLERANCE_PCT,   # kept for the fail-fast check below only,
            "invalid_currencyDate_rows":0
        }                                                # "_" prefix = not written to any table
        all_summary_rows.append(row)
    else:
        for x in all_summary_rows:
            if x.get("source_table") == source_table:
                x["invalid_currencyDate_rows"] = bad_rows
                x["valid_rows"] = x["valid_rows"] - bad_rows
        row = {}
    return row

# COMMAND ----------

# MAGIC %md
# MAGIC ##SILVER TRANSFORMATION

# COMMAND ----------

# DBTITLE 1,Cell 16
for row in df_tblNames.collect():

    table_name = row["table_name"]
    print(f"Processing table: {table_name}")

    config_rows = df_config.filter(col('table_name') == table_name)
    # Get required columns from config
    arr_cols= [
    r["column_name"]
    for r in (
        config_rows.where(col("table_name") == table_name).select("column_name").collect()
    )
    ]
    arr_cols.extend(["recordstamp", "is_deleted", "_ingested_at"])
    
    # Read Bronze table
    df_tbl = spark.read.table(f"{BRONZE_SCHEMA}.{table_name}")

    #================STEP 1: SELECT REQUIRED FIELDS ONLY & IS_DELETED set to false=================
    df_tbl = df_tbl.where(col("is_deleted") == False).select(*arr_cols)

    # SAVE AS stage_1 TABLE
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true").\
        saveAsTable(f"{SILVER_SCHEMA}.{table_name}_stg1"
        )

    #========================DROP NULLS================================
    non_nullable_cols= [
    r["column_name"]
    for r in (config_rows
        .where(col("IsNullable") == False)
        .select("column_name")
        .collect()
    )
    ]
    df_tbl = df_tbl.dropna(subset=non_nullable_cols)
    #df_tbl=drop_nullable_fields(df_tbl,nullable_cols, table_name)
    # SAVE AS stage_2 TABLE
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}_stg2"
        )
    
    #=============NORMALIZING KEYS=======================
    normalize_config=df_config.filter((col('table_name') == table_name) & (col("Normalize_Length").isNotNull())).\
        select ("column_name","Normalize_Length")

    for row in normalize_config.collect():
        df_tbl = df_tbl.withColumn(row.column_name,\
            F.lpad(F.trim(F.col(row.column_name)),row.Normalize_Length,"0"))
    
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
         f"{SILVER_SCHEMA}.{table_name}_stg3")
    
    
    # =====================STEP 3: DUPLICATE RECORDS======================
    unique_cols= [
    r["column_name"]
    for r in (config_rows
        .where(col("IsKey") == True)
        .select("column_name")
        .collect()
    )
    ]

    df_tbl = dedup_latest_change_wins(df_tbl, unique_cols, table_name)
    # SAVE AS stage_3 TABLE
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}_stg4"
        )

     #========================STEP 4: TYPE CAST==================================================
    date_cols= [
    r["column_name"]
    for r in (
        config_rows
        .where(col("Data_Type") == "DATE")
        .select("column_name")
        .collect()
    )
    ]
    
    for date_col in date_cols:                       
        df_tbl = decode_sap_date(df_tbl, date_col)

    decimal_cols = {
    r["column_name"]: r["Data_Type"]
    for r in (
        config_rows
        .where(col("Data_Type").like("DECIMAL%") | (col("Data_Type") == "INTEGER"))
        .select("column_name", "Data_Type")
        .collect()
    )
    }

    for column_name, data_type in decimal_cols.items():                      
        df_tbl = df_tbl.withColumn(column_name, F.col(column_name).cast(data_type))

    # SAVE AS stage_4 TABLE
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
         f"{SILVER_SCHEMA}.{table_name}_stg5"
        )
    #=======================VALUE RANGE============================
    positive_cols = [
    r["column_name"]
    for r in (
        config_rows
        .where(((col("Data_Type").like("DECIMAL%")) | (col("Data_Type") == "INTEGER")) & (col("IsNegative") == False))
        .select("column_name")
        .collect()
    )
    ]
    for column_name in positive_cols:                      
        df_tbl = df_tbl.where(col(f"{column_name}") >= 0)

    # SAVE AS stage_5 TABLE
    df_tbl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
         f"{SILVER_SCHEMA}.{table_name}_stg6")
    
    #==============TABLE SPECIFIC VALIDATIONS============
    match table_name:
        case "MAKT":
            df_tbl.filter(F.col("SPRAS") == "E").write.mode("overwrite").\
                option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.{table_name}_stg7")

        case "TCURR" | "TCURF":
            df_tbl = df_tbl.filter(F.col("KURST") == "M")
            df_tbl = df_tbl.withColumn("corrected_date",99999999 - F.col("GDATU"))
            df_tbl = df_tbl.withColumn("valid_from_date",\
                F.to_date(F.col("corrected_date").cast("string"), "yyyyMMdd"))

            df_tbl = df_tbl.orderBy(F.col("valid_from_date").desc())
            df_tbl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.{table_name}_stg7")
            dfs[f"df_{table_name}"] = df_tbl
    
    #==============STORING FOR INTEGRITY CHECK==============
    if (table_name in (key for key in dict_hierarchy.keys()) or 
        table_name in (value for value in dict_hierarchy.values())):
        dfs[f"df_{table_name}"] = df_tbl
    else:
        df_tbl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.{table_name}_clean1")


    del df_tbl

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 17
all_exceptions_dfs = []
all_summary_rows = [] 
TOLERANCE_PCT = 0.5

#all_exceptions = None

for item in df_hierarchy_config.collect():
    parent =item["Header_Table"]
    child = item["Item_Table"]
    key_columns = [r["column_name"] for r in df_config.\
            where((col("table_name") == parent) & (col("IsKey") == True)).\
                select("column_name").collect()]
    curr_conv_reqd = item["Currency_conversion"]

    df_parent=dfs[f"df_{parent}"]
    df_child=dfs[f"df_{child}"]

    #===============REFERENTIAL INTEGRITY============
    df_child=referential_integrity_check(df_child, df_parent, key_columns[0], child, parent)
    df_child.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.{child}_stg7")
    df_parent.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.{parent}_stg7")
    #===============CURRENCY CONVERSION============

    if(curr_conv_reqd):
        
        child_key_columns = [r["column_name"] for r in df_config.\
            where((col("table_name") == child) & (col("IsKey") == True)).\
                select("column_name").collect()]
        curr_conv_date = item["Currency_conversion_date"]

        convert_currency(
            parent_df=df_parent,
            child_df=df_child,
            parent_table_name=parent,
            parent_key_col=key_columns,
            from_currency_col="WAERK",
            transaction_date_col=curr_conv_date,
            to_currency="USD",
            tcurr_df=dfs["df_TCURR"],
            tcurf_df=dfs["df_TCURF"],
            child_table_name=child,
            child_key_cols=child_key_columns
        )
    else:
        df_child.write.mode("overwrite").option("overwriteSchema", "true")\
            .saveAsTable(f"{SILVER_SCHEMA}.{child}_clean1")
        df_parent.write.mode("overwrite").option("overwriteSchema", "true")\
            .saveAsTable(f"{SILVER_SCHEMA}.{parent}_clean1")

# COMMAND ----------

# DBTITLE 1,Cell 20

exception_schema_cols = ["exception_id", "rule_name", "source_table", "parent_table", "source_key",
                          "reason_code", "reason_description", "detected_at", "pipeline_run_id", "load_date"]

summary_schema = StructType([
    StructField("run_id", StringType()), StructField("rule_name", StringType()),
    StructField("source_table", StringType()), StructField("parent_table", StringType()),
    StructField("total_child_rows", LongType()), StructField("orphan_rows", LongType()),
    StructField("valid_rows", LongType()), StructField("orphan_pct", DoubleType()),
    StructField("run_timestamp", TimestampType()), StructField("load_date", StringType()),
    StructField("invalid_currencyDate_rows", LongType())
    ])

from functools import reduce
exception_df = reduce(lambda a, b: a.union(b),
                      [df.select(exception_schema_cols) for df in all_exceptions_dfs]) \
                   .withColumn("load_date", F.col("load_date").cast("date"))
summary_df = spark.createDataFrame(all_summary_rows, schema=summary_schema) \
                   .withColumn("load_date", F.col("load_date").cast("date"))

exception_df.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.dq_exceptions")
summary_df.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true")\
        .saveAsTable(f"{SILVER_SCHEMA}.dq_reconciliation_summary")
