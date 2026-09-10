# Databricks notebook source
dbutils.widgets.text("catalog", "sdotc_dev_dbw")

# COMMAND ----------

CATALOG=dbutils.widgets.get("catalog")
DEFAULT_SCHEMA=f"{CATALOG}.default"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA=f"{CATALOG}.gold"
#dbutils.widgets.text("SILVER_SCHEMA", f"{SILVER_SCHEMA}")

# COMMAND ----------

from pyspark.sql.window import Window
#from pyspark.sql.functions import row_number
from datetime import datetime,date, timedelta
from pyspark.sql.functions import current_date, lit
from pyspark.sql.functions import col, min, max
import builtins
from pyspark.sql.functions import col, min, max, date_format, year, quarter, month, weekofyear, dayofmonth, explode, sequence,dayofweek,monotonically_increasing_id
from pyspark.sql import Row



# COMMAND ----------

customer_df = spark.read.table(f"{SILVER_SCHEMA}.kna1_clean1")
silver_mara_df = spark.read.table(f"{SILVER_SCHEMA}.mara_clean1")
silver_makt_df = spark.read.table(f"{SILVER_SCHEMA}.makt_clean1")
tcurr = spark.table(f"{SILVER_SCHEMA}.tcurr_clean1")
tcurf = spark.table(f"{SILVER_SCHEMA}.tcurf_clean1")
vbak_raw = spark.table(f"{SILVER_SCHEMA}.vbak_clean1")
df_config = spark.read.table(f"{DEFAULT_SCHEMA}.Table_Columns_Config")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer (SCD type 2)
# MAGIC Every Dimension should contain:Customer_SK,Start_Date,End_Date,Is_Current

# COMMAND ----------

# DBTITLE 1,Cell 6
spark.sql(f"""
UPDATE {SILVER_SCHEMA}.kna1_clean1
SET ort01 = 'Territory New'
WHERE kunnr = '0000000001'
""")

# COMMAND ----------


silver_dimcustomer_df = customer_df.select(
    "KUNNR",
    "NAME1",
    "LAND1",
    "ORT01",
    "REGIO",
    "KTOKD"
)
silver_dimcustomer_df = silver_dimcustomer_df\
    .withColumnRenamed("KUNNR","Customer_ID")\
    .withColumnRenamed("NAME1","Customer_Name")\
    .withColumnRenamed("LAND1","Country")\
    .withColumnRenamed("ORT01","City")\
    .withColumnRenamed("REGIO","Region")\
    .withColumnRenamed("KTOKD","Account_Group")\
    .withColumn("Start_Date", current_date())\
    .withColumn("End_Date", lit(None).cast("date"))\
    .withColumn("Is_Current", lit(True))

# COMMAND ----------

#create SURROGATE KEY
#window_spec = Window.orderBy("Customer_ID")
silver_dimcustomer_df=silver_dimcustomer_df.orderBy("Customer_ID")
silver_dimcustomer_df = silver_dimcustomer_df.withColumn(
    "Customer_SK",
    monotonically_increasing_id()+1
    #row_number().over(window_spec)
)

#display(silver_dimcustomer_df)

# COMMAND ----------

# DBTITLE 1,Cell 9

from pyspark.sql.functions import col

table_exists = spark.catalog.tableExists(
    f"{GOLD_SCHEMA}.dim_customer1"
)
existing_dimcustomer_df = spark.createDataFrame([], silver_dimcustomer_df.schema)

if not table_exists:

    print("Initial Load Started...")

    silver_dimcustomer_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_customer")

    print("Gold Table Created Successfully.")
   


else:

    # Read the existing gold table

    existing_dimcustomer_df = spark.read.table(f"{GOLD_SCHEMA}.dim_customer1")

    # Filter the current record from the existing gold table

    existing_dimcustomer_df = existing_dimcustomer_df.filter(
        col("Is_Current") == True
    )

# Compare the new and existing gold table

compare_df = silver_dimcustomer_df.alias("new").join(
    existing_dimcustomer_df.alias("old"),
    on="Customer_ID",
    how="left"
    )

# Find New Customers that not present in the gold table
    
new_customer_df = compare_df.filter(
    col("old.Customer_ID").isNull()
    )

# COMMAND ----------

# find changed customers
changed_customer_df = compare_df.filter(

    (col("old.Customer_ID").isNotNull()) &

    (
        (col("new.Customer_Name") != col("old.Customer_Name")) |
        (col("new.Country") != col("old.Country")) |
        (col("new.City") != col("old.City")) |
        (col("new.Region") != col("old.Region")) |
        (col("new.Account_Group") != col("old.Account_Group"))
    )
)

# COMMAND ----------

#Expire old records
from pyspark.sql.functions import when, current_date

# Get the list of changed Customer IDs
changed_customer_ids = [ 
    row.Customer_ID
    for row in changed_customer_df.select("Customer_ID").distinct().collect()
]

# Expire old records
expired_customer_df = existing_dimcustomer_df.withColumn(
    "Is_Current",
    when(col("Customer_ID").isin(changed_customer_ids), False)
    .otherwise(col("Is_Current"))
).withColumn(
    "End_Date",
    when(col("Customer_ID").isin(changed_customer_ids), current_date())
    .otherwise(col("End_Date"))
)

# COMMAND ----------

#Insert updated new version
# Get current maximum surrogate key
max_sk = existing_dimcustomer_df.agg({"Customer_SK":"max"}).collect()[0][0]

if max_sk is None:
    max_sk = 0

window_spec = Window.orderBy("Customer_ID")

changed_new_df = changed_customer_df.select("new.*") \
.withColumn(
    "Customer_SK",
    #row_number().over(window_spec)
    monotonically_increasing_id()+1 + max_sk
) \
.withColumn(
    "Start_Date",
    current_date()
) \
.withColumn(
    "End_Date",
    lit(None).cast("date")
) \
.withColumn(
    "Is_Current",
    lit(True)
)

# COMMAND ----------

#prepare new customers
new_customer_df = new_customer_df.select("new.*") \
.withColumn(
    "Customer_SK",
    monotonically_increasing_id()+1
    #row_number().over(window_spec) 
    + max_sk + changed_new_df.count()
) \
.withColumn(
    "Start_Date",
    current_date()
) \
.withColumn(
    "End_Date",
    lit(None).cast("date")
) \
.withColumn(
    "Is_Current",
    lit(True)
)
#combine
final_dimcustomer_df = expired_customer_df \
.unionByName(changed_new_df, allowMissingColumns=True) \
.unionByName(new_customer_df, allowMissingColumns=True)

final_dimcustomer_df.write.format("delta")\
    .mode("overwrite").option("overwriteSchema", "true")\
    .saveAsTable(f"{GOLD_SCHEMA}.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ###MATERIAL

# COMMAND ----------

#prepare tables
silver_material_df = silver_mara_df.alias("mara") \
.join(
    silver_makt_df.alias("makt"),
    on="MATNR",
    how="left"
)

silver_material_df = silver_material_df.select(
    "MATNR",
    "MAKTX",
    "MTART",
    "MATKL",
    "MEINS"
)

silver_material_df = silver_material_df \
    .withColumnRenamed("MATNR", "Material_ID") \
    .withColumnRenamed("MAKTX", "Material_Name") \
    .withColumnRenamed("MTART", "Material_Type") \
    .withColumnRenamed("MATKL", "Material_Group") \
    .withColumnRenamed("MEINS", "Base_Unit")\
    .withColumn("Start_Date", current_date()) \
    .withColumn("End_Date", lit(None).cast("date")) \
    .withColumn("Is_Current", lit(True))\
    .orderBy("Material_ID")

#SCD implementation
silver_material_df = silver_material_df.withColumn(
    "Material_SK",
    monotonically_increasing_id()+1
    #row_number().over(window_spec)
)


# COMMAND ----------

table_exists = spark.catalog.tableExists(f"{GOLD_SCHEMA}.dim_material")  
#initial load vs incremental load
if not table_exists:
    print("Initial Load Started...")

    silver_material_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_material")

    print("silver dim_material Table Created Successfully.")

else:

    print("Silver Table Already Exists.")
    print("Running Incremental Load...")

    # Read the existing table
    full_existing_df = spark.read.table(f"{GOLD_SCHEMA}.dim_material")

    # Split into current and historical records
    existing_dimmaterial_df = full_existing_df.filter(col("Is_Current") == True)
    historical_df = full_existing_df.filter(col("Is_Current") == False)

    # Compare the new and existing data
    compare_df = silver_material_df.alias("new").join(
        existing_dimmaterial_df.alias("old"),
        on="Material_ID",
        how="left"
    )

    # Find materials that are not present in the existing table
    new_material_df = compare_df.filter(col("old.Material_ID").isNull())

    if(new_material_df.count()>0):
        # Find records where a tracked attribute changed
        changed_materials_df = compare_df.filter((col("old.Material_ID").isNull()) &\
            ((col("new.Material_Name") != col("old.Material_Name")) |
            (col("new.Material_Type") != col("old.Material_Type")) |
            (col("new.Material_Group") != col("old.Material_Group")) |
            (col("new.Base_Unit") != col("old.Base_Unit"))
            )
        )

        changed_material_ids = [row.Material_ID\
            for row in changed_materials_df.select(col("old.Material_ID")).distinct().collect()]

        # Expire old records
        expired_material_df = existing_dimmaterial_df.withColumn("Is_Current",\
            when(col("Material_ID").isin(changed_material_ids), False).otherwise(col("Is_Current")))\
            .withColumn("End_Date",\
            when(col("Material_ID").isin(changed_material_ids), current_date())\
            .otherwise(col("End_Date"))
        )
        # Get current maximum surrogate key
        max_sk = existing_dimmaterial_df.agg({"Material_SK": "max"}).collect()[0][0]

        if max_sk is None: max_sk = 0

        changed_new_df = changed_materials_df.select("new.*") \
            .withColumn("Material_SK", monotonically_increasing_id()+1 + max_sk) \
            .withColumn("Start_Date", current_date()) \
            .withColumn("End_Date", lit(None).cast("date")) \
            .withColumn("Is_Current", lit(True))

        new_material_df = new_material_df.select("new.*") \
            .withColumn("Material_SK", monotonically_increasing_id()+1 + max_sk + changed_new_df.count()) \
            .withColumn("Start_Date", current_date()) \
            .withColumn("End_Date", lit(None).cast("date")) \
            .withColumn("Is_Current", lit(True))

        final_dimmaterial_df = expired_material_df \
            .unionByName(changed_new_df, allowMissingColumns=True) \
            .unionByName(new_material_df, allowMissingColumns=True) \
            .unionByName(historical_df, allowMissingColumns=True)

        #save to Gold layer
        final_dimmaterial_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"{GOLD_SCHEMA}.dim_material")


# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE

# COMMAND ----------

# DBTITLE 1,Cell 18
min_dates,max_dates=[],[]
date_config= df_config.filter(col("Data_Type") == "DATE").select("Table_Name","Column_Name")
date_tbls=date_config.select("Table_Name").distinct()
additional_tbls = spark.createDataFrame(
    [Row(Table_Name="TCURR"), Row(Table_Name="TCURF")]
)

date_tbls = date_tbls.union(additional_tbls)#.distinct()
#date_tbls.append("TCURR","TCURF")

for tbl in date_tbls.collect():
    date_cols = date_config.filter(col("Table_Name") == tbl[0]).select("Column_Name").collect()
    if tbl[0] == "TCURR" or tbl[0] == "TCURF":
        date_cols = date_cols + [Row(Column_Name="valid_from_date")]
        #date_cols.append("valid_from_date")
    df = spark.table(f"{SILVER_SCHEMA}.{tbl[0]}_clean1")
    for col_row in date_cols:
        result = df.agg(
            min(col(col_row[0])).alias("mn"),
            max(col(col_row[0])).alias("mx")
        ).first()
        min_dates.append(result["mn"])
        max_dates.append(result["mx"])

print(f"Min date: {min_dates}")
print(f"Max date: {max_dates}")
overall_min = builtins.min(min_dates) if min_dates else None
overall_max = builtins.max(max_dates) if max_dates else None

overall_min = datetime.strptime(str(overall_min), "%Y-%m-%d").date()
overall_max = datetime.strptime(str(overall_max), "%Y-%m-%d").date()

range_start = date(overall_min.year, 1, 1)
range_end = date(overall_max.year + 1, 12, 31)
print(f"Dim_Date will cover: {range_start} to {range_end}")

#genarate calendar
calendar_df = spark.sql(f"""
    SELECT explode(sequence(to_date('{range_start}'), to_date('{range_end}'), interval 1 day)) AS calendar_date
""")

#surrogate key
dim_date = (
    calendar_df
    .withColumn("date_sk", date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year_num", year("calendar_date"))
    .withColumn("quarter_num", quarter("calendar_date"))
    .withColumn("month_num", month("calendar_date"))
    .withColumn("month_name", date_format("calendar_date", "MMMM"))
    .withColumn("week_of_year", weekofyear("calendar_date"))
    .withColumn("day_of_month", dayofmonth("calendar_date"))
    .withColumn("day_name", date_format("calendar_date", "EEEE"))
    
    # Spark's dayofweek(): 1=Sunday..7=Saturday. Converted here to the more
    # common reporting convention: 1=Monday..7=Sunday.
    .withColumn("day_of_week_num", ((dayofweek("calendar_date") + 5) % 7) + 1)
    .withColumn("is_weekend", col("day_of_week_num").isin([6, 7]))
    .select("date_sk", "calendar_date", "year_num", "quarter_num", "month_num","month_name",\
        "week_of_year", "day_of_month", "day_name","day_of_week_num", "is_weekend",
    )
)
#save DIM table
(dim_date.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{GOLD_SCHEMA}.dim_date"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###CURRENCY RATE

# COMMAND ----------

join_keys = ["kurst", "fcurr", "tcurr", "gdatu"]
dim_currency_rate_stage = (
    tcurr.alias("r")
    .join(tcurf.alias("f"), on=join_keys, how="left")   # LEFT: keep every rate even if no matching ratio row
    .select(
        col("r.kurst").alias("kurst"),
        col("r.fcurr").alias("from_currency"),
        col("r.tcurr").alias("to_currency"),
        col("r.valid_from_date").alias("rate_date"),          # NULL until gdatu is resolved — see RISK-001     
        col("r.ukurs").alias("exchange_rate"),
        col("f.ffact").alias("ratio_factor_from"),
        col("f.tfact").alias("ratio_factor_to"),
    )
)
#Surrogate Key
dim_currency_rate_stage=dim_currency_rate_stage.orderBy("kurst", "from_currency", "to_currency", "rate_date")
dim_currency_rate_silver = (
    dim_currency_rate_stage
    .withColumn("currency_rate_sk",
                #row_number().over(Window.orderBy("kurst", "from_currency", "to_currency", )
                monotonically_increasing_id()+1)
    .select("currency_rate_sk", "kurst", "from_currency", "to_currency",
            "rate_date", "exchange_rate", "ratio_factor_from", "ratio_factor_to")
)
(dim_currency_rate_silver.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.dim_currency_rate1"))

#display(dim_currency_rate_silver)


#adding Date_sk
lookup_dated = dim_currency_rate_silver.withColumn(
    "rate_date_sk_lookup",
    date_format(col("rate_date"), "yyyyMMdd").cast("int")) 

dim_currency_rate=lookup_dated.join(dim_date, lookup_dated.rate_date_sk_lookup == dim_date.date_sk, how="left")
#.drop("rate_date_sk_lookup")

dim_currency_rate=dim_currency_rate.select("currency_rate_sk", "kurst", "from_currency",\
    "to_currency","date_sk", "exchange_rate", "ratio_factor_from", "ratio_factor_to").\
    withColumnRenamed("date_sk", "rate_date_sk")

dim_currency_rate.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{GOLD_SCHEMA}.dim_currency_rate")

#display(dim_currency_rate)


# COMMAND ----------

# MAGIC %md
# MAGIC ###SALES ORGANIZATION

# COMMAND ----------

dim_sales_org_stage = (
    vbak_raw
    .filter(col("vkorg").isNotNull() & col("vtweg").isNotNull() & col("spart").isNotNull())
    .select("vkorg", "vtweg", "spart")
    .distinct()
)

# COMMAND ----------

#window_spec_sales_org = Window.orderBy("vkorg","vtweg","spart")

dim_sales_organization = (
    dim_sales_org_stage
    .withColumn(
        "sales_org_sk",
        monotonically_increasing_id()+1
        #row_number().over(window_spec_sales_org)
    )
    .select("sales_org_sk","vkorg","vtweg","spart")
)

dim_sales_organization.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{GOLD_SCHEMA}.dim_sales_organization")