# Databricks notebook source
dbutils.widgets.text("catalog", "sdotc_dev_dbw")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

vbak_df = spark.table(f"{SILVER_SCHEMA}.vbak_clean1")
vbap_df = spark.table(f"{SILVER_SCHEMA}.vbap_clean1")

likp = spark.table(f"{SILVER_SCHEMA}.likp_clean1")
lips = spark.table(f"{SILVER_SCHEMA}.lips_clean1")

vbrk = spark.table(f"{SILVER_SCHEMA}.vbrk_clean1")
vbrp = spark.table(f"{SILVER_SCHEMA}.vbrp_clean1")

vbfa = spark.table(f"{SILVER_SCHEMA}.vbfa_clean1")

dim_sales_organization = spark.table(f"{GOLD_SCHEMA}.dim_sales_organization")
dim_customer     = spark.table(f"{GOLD_SCHEMA}.dim_customer").filter(F.col("Is_Current") == True)
dim_material     = spark.table(f"{GOLD_SCHEMA}.dim_material").filter(F.col("Is_Current") == True)
dim_date         = spark.table(f"{GOLD_SCHEMA}.dim_date")
dim_currency_rate = spark.table(f"{GOLD_SCHEMA}.dim_currency_rate")


sales_order_fact = spark.table(f"{SILVER_SCHEMA}.fact_sales_order")
delivery_fact = spark.table(f"{SILVER_SCHEMA}.fact_delivery")
#open_order_value = spark.table(f"{SILVER}.open_order_value")

fact_sales_order_gold_final = spark.table(f"{GOLD_SCHEMA}.fact_sales_order_gold")
fact_billing_gold_final = spark.table(f"{GOLD_SCHEMA}.fact_billing_gold")
fact_delivery_gold_final = spark.table(f"{GOLD_SCHEMA}.fact_delivery_gold")


# COMMAND ----------

ORDER, DELIVERY, INVOICE = "C", "J", "M"
GROUP_CURRENCY = "USD" 
CONFIG = {
    "FACT_CUSTOMER_COL": "customer",
    "CUSTOMER_DIM_KEY": "customer",
    "CUSTOMER_IS_CURRENT_COL": "Is_Current",
 
    "FACT_MATERIAL_COL": "material",
    "MATERIAL_DIM_KEY": "Material_ID",
    "MATERIAL_IS_CURRENT_COL": "Is_Current",
 
    "FACT_SALES_ORG_COL": "sales_org",
    "SALES_ORG_DIM_KEYS": "sales_org",   # single-column key: fact.sales_org == dim.vkorg
 
    "FACT_DATE_COL": "order_date",
    "DATE_DIM_KEY": "date_sk",
    "DATE_DIM_FULLDATE_COL": "full_date",
}
 

# COMMAND ----------

# MAGIC %md
# MAGIC ###SALES FACT TABLE

# COMMAND ----------

sales_order_joined = (
    vbap_df.alias("item")
    .join(
        vbak_df.alias("header"),
        on=[
            F.col("item.MANDT") == F.col("header.MANDT"),
            F.col("item.VBELN") == F.col("header.VBELN")
        ],
        how="inner"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 8
fact_sales_order = (
    sales_order_joined
    .select(
        F.col("header.MANDT").alias("mandt"),
        F.col("item.VBELN").alias("sales_order"),
        F.col("item.posnr").alias("sales_order_item"),
        F.col("header.erdat").alias("order_date"),
        F.col("header.kunnr").alias("customer"),
        F.col("header.vkorg").alias("sales_org"),
        F.col("header.vtweg").alias("distr_channel"),
        F.col("header.spart").alias("division"),
        F.col("item.matnr").alias("material"),
        F.col("item.kwmeng").alias("order_quantity"),
        F.col("item.netwr").alias("item_net_value"),
        F.col("item.waerk").alias("source_currency"),
        F.col("header.exchange_rate_date").alias("rate_date"),
        F.col("header.exchange_rate").alias("exchange_rate"),
        F.lit("USD").alias("group_currency"),
        (F.col("header.exchange_rate") * F.col("item.netwr")*F.col("header.conversion_ratio")).alias("converted_value_usd")
    )
)

fact_sales_order.write.mode("overwrite")\
    .option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.fact_sales_order")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DELIVERY FACT TABLE

# COMMAND ----------

# 1. Prepare LIKP delivery headers
likp_prepared = (
    likp
    .select(
        "mandt",
        F.col("vbeln").alias("delivery_number"),
        F.col("erdat").alias("delivery_created_date"),
        F.col("wadat").alias("planned_goods_issue_date"),
        F.col("wadat_ist").alias("actual_goods_issue_date"),
        F.col("lfdat").alias("planned_delivery_date")
    )
)

# 2. Prepare LIPS delivery items and the sales-order reference
lips_prepared = (
    lips
    .select(
        "mandt",
        F.col("vbeln").alias("delivery_number"),
        F.col("posnr").alias("delivery_item"),
        F.col("vgbel").alias("sales_order"),
        F.col("vgpos").alias("sales_order_item"),
        F.col("matnr").alias("material"),
        F.col("lfimg").alias("delivered_quantity")
    )
)

# 3. Prepare the validated Gold Sales Order Fact for the item-level link
orders_prepared = (
    sales_order_fact
    .select(
        F.col("mandt"),
        F.col("sales_order").alias("sales_order"),
        F.col("sales_order_item").alias("sales_order_item"),
        F.col("order_date").alias("order_created_date")
    )
   # .dropDuplicates(["mandt", "sales_order", "sales_order_item"])
)

# 4. Build Fact_Delivery
fact_delivery_gold = (
    lips_prepared.alias("l")
    .join(
        likp_prepared.alias("h"),
        on=[
            F.col("l.mandt") == F.col("h.mandt"),
            F.col("l.delivery_number") == F.col("h.delivery_number")
        ],
        how="left"
    )
    .join(
        orders_prepared.alias("o"),
        on=[
            F.col("l.mandt") == F.col("o.mandt"),
            F.col("l.sales_order") == F.col("o.sales_order"),
            F.col("l.sales_order_item") == F.col("o.sales_order_item")
        ],
        how="inner" #changed from left
    )
    .select(
        F.col("l.mandt").alias("mandt"),
        F.col("l.delivery_number").alias("delivery_number"),
        F.col("l.delivery_item").alias("delivery_item"),
        F.col("l.sales_order").alias("sales_order"),
        F.col("l.sales_order_item").alias("sales_order_item"),
        F.col("l.material").alias("material"),
        F.col("l.delivered_quantity").alias("delivered_quantity"),

        F.col("o.order_created_date").alias("order_created_date"),
        F.col("h.delivery_created_date").alias("delivery_created_date"),
        F.col("h.planned_goods_issue_date").alias("planned_goods_issue_date"),
        F.col("h.actual_goods_issue_date").alias("actual_goods_issue_date"),
        F.col("h.planned_delivery_date").alias("planned_delivery_date"),
        
        F.when(
            F.col("o.sales_order").isNull(),
            "UNMATCHED_ORDER"
        ).otherwise("ORDER_MATCHED").alias("order_link_status"),
        
        F.when(
            F.col("h.actual_goods_issue_date").isNotNull(),
            "ISSUED"
        ).otherwise("NOT_ISSUED").alias("delivery_status"),

        F.when(
            F.col("h.actual_goods_issue_date").isNull(),
            "INVALID_ACTUAL_GI_DATE"
        ).when(
            F.col("o.order_created_date").isNull() |
            F.col("h.actual_goods_issue_date").isNull(),
            "MISSING_DATE"
        ).when(
            F.datediff(
                F.col("h.actual_goods_issue_date"),
                F.col("o.order_created_date")
            ) < 0,
            "DATE_ANOMALY"
        ).otherwise("VALID").alias("lead_time_status"),

        F.datediff(
            F.col("h.actual_goods_issue_date"),
            F.col("o.order_created_date")
        ).alias("order_to_delivery_lead_days")
    )
)


# COMMAND ----------

(
    fact_delivery_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.fact_delivery")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ###BILLING FACT TABLE

# COMMAND ----------

# Prepare VBRK billing headers
vbrk_prepared = (
    vbrk
    .select(
        "mandt",
        F.col("vbeln").alias("billing_number"),
        F.col("fkdat").alias("billing_date"),
        F.col("waerk").alias("source_currency"),
        F.col("vkorg").alias("sales_org"),
        F.col("vtweg").alias("distribution_channel"),   # add
        F.col("spart").alias("division"),               # add
        F.col("kunag").alias("sold_to_customer"),
        F.col("kunrg").alias("payer_customer"),
        "exchange_rate",
        "exchange_rate_date",
        "conversion_ratio"
    )
)

# Prepare VBRP billing items
vbrp_prepared = (
    vbrp
    .select(
        F.col("mandt"),
        F.col("vbeln").alias("billing_number"),
        F.col("posnr").alias("billing_item"),
        F.col("vgbel").alias("reference_number"),
        F.col("vgpos").alias("reference_item"),
        F.col("vgtyp").alias("preceding_document_type"),
        F.col("matnr").alias("material"),
        F.col("fkimg").alias("billed_quantity"),
        F.col("netwr").alias("invoiced_revenue_local")
    )
)

# Keep relevant lineage fields from Delivery Fact
delivery_lineage = (
    delivery_fact
    .select(
        "mandt",
        F.col("delivery_number").alias("reference_number"),
        F.col("delivery_item").alias("reference_item"),
        F.col("delivery_number").alias("delivery_number"),
        F.col("delivery_item").alias("delivery_item"),
        F.col("sales_order").alias("delivery_sales_order"),
        F.col("sales_order_item").alias("delivery_sales_order_item")
    )
    #.dropDuplicates()
    .withColumn("delivery_match", F.lit(1))
)

# Keep relevant lineage fields from Sales Order Fact
order_lineage = (
    sales_order_fact
    .select(
        "mandt",
        F.col("sales_order").alias("reference_number"),
        F.col("sales_order_item").alias("reference_item"),
        F.col("sales_order").alias("direct_sales_order"),
        F.col("sales_order_item").alias("direct_sales_order_item")
    )
    #.dropDuplicates()
    .withColumn("order_match", F.lit(1))
)

# Build the item-level Billing Fact; currency conversion is the next step
billing_fact_base = (
    vbrp_prepared.alias("i")
    .join(
        vbrk_prepared.alias("h"),
        on=[
            F.col("i.mandt") == F.col("h.mandt"),
            F.col("i.billing_number") == F.col("h.billing_number")
        ],
        how="left"
    )
    .join(
        delivery_lineage.alias("d"),
        on=[
            F.col("i.mandt") == F.col("d.mandt"),
            F.col("i.reference_number") == F.col("d.reference_number"),
            F.col("i.reference_item") == F.col("d.reference_item")
        ],
        how="left"
    )
    .join(
        order_lineage.alias("o"),
        on=[
            F.col("i.mandt") == F.col("o.mandt"),
            F.col("i.reference_number") == F.col("o.reference_number"),
            F.col("i.reference_item") == F.col("o.reference_item")
        ],
        how="left"
    )
    .select(
        F.col("i.mandt").alias("mandt"),
        F.col("i.billing_number").alias("billing_number"),
        F.col("i.billing_item").alias("billing_item"),
        F.col("h.billing_date").alias("billing_date"),
        F.col("h.source_currency").alias("source_currency"),
        F.col("h.sales_org").alias("sales_org"),
        F.col("h.distribution_channel").alias("distribution_channel"),   # add
        F.col("h.division").alias("division"),                          # add
        F.col("h.sold_to_customer").alias("sold_to_customer"),
        F.col("h.payer_customer").alias("payer_customer"),

        F.col("i.material").alias("material"),
        F.col("i.billed_quantity").alias("billed_quantity"),
        F.col("i.invoiced_revenue_local").alias("invoiced_revenue_local"),

        F.col("i.reference_number").alias("reference_number"),
        F.col("i.reference_item").alias("reference_item"),
        F.col("i.preceding_document_type").alias("preceding_document_type"),

        F.col("d.delivery_number").alias("delivery_number"),
        F.col("d.delivery_item").alias("delivery_item"),
        F.col("h.exchange_rate"),
        F.col("h.conversion_ratio"),
        F.col("h.exchange_rate_date").alias("rate_date"),
        F.coalesce(
            F.col("d.delivery_sales_order"),
            F.col("o.direct_sales_order")
        ).alias("sales_order"),
        F.coalesce(
            F.col("d.delivery_sales_order_item"),
            F.col("o.direct_sales_order_item")
        ).alias("sales_order_item"),

        F.when(F.col("h.billing_number").isNull(), "MISSING_BILLING_HEADER")
         .otherwise("HEADER_MATCHED").alias("billing_header_status"),

        F.when(F.col("i.reference_number").isNull(), "NO_REFERENCE")
         .when(F.col("d.delivery_match").isNotNull(), "DELIVERY_MATCHED")
         .when(F.col("o.order_match").isNotNull(), "ORDER_MATCHED")
         .otherwise("UNMATCHED_REFERENCE").alias("reference_link_status")
    )
    .withColumn(
        "group_currency",F.lit("USD"))
    .withColumn(
        "invoiced_revenue_group",
        F.when(
            F.col("invoiced_revenue_local").isNotNull() &
            F.col("exchange_rate").isNotNull(),
            (
                F.col("invoiced_revenue_local").cast("double") *
                F.col("exchange_rate")* F.col("conversion_ratio")
            ).cast("decimal(18,2)")
        )
    )
)


# COMMAND ----------


fact_billing_joined = billing_fact_base.join(
    F.broadcast(dim_sales_organization.select(
        "sales_org_sk", "vkorg", "vtweg", "spart"
    )),
    on=[
        billing_fact_base.sales_org == F.col("vkorg"),
        billing_fact_base.distribution_channel == F.col("vtweg"),
        billing_fact_base.division == F.col("spart")
    ],
    how="left"
)

fact_billing_gold = fact_billing_joined.select(
    "mandt",
    "billing_number",
    "billing_item",
    "billing_date",

    "source_currency",
    "group_currency",
    "exchange_rate",
    "rate_date",
    #"currency_conversion_status",

    "sales_org_sk",

    "sold_to_customer",
    "payer_customer",

    "material",
    "billed_quantity",
    "invoiced_revenue_local",
    "invoiced_revenue_group",

    "reference_number",
    "reference_item",
    "preceding_document_type",
    "reference_link_status",

    "delivery_number",
    "delivery_item",
    "sales_order",
    "sales_order_item",
    "distribution_channel",
    "division",
    "billing_header_status"
)


# COMMAND ----------

fact_billing_gold.write.format("delta").mode("overwrite").\
    option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.fact_billing")



# COMMAND ----------

# MAGIC %md
# MAGIC ###ORDER TO CASH

# COMMAND ----------

vbap = vbap_df.select(
    F.col("vbeln").alias("order_vbeln"),
    F.col("posnr").alias("order_posnr"),
    F.col("matnr").alias("order_matnr"),
    F.col("netwr").alias("order_value"),
    F.col("kwmeng").alias("order_qty"),
).withColumn(
    "unit_price",
    F.when(F.col("order_qty") > 0, F.col("order_value") / F.col("order_qty")).otherwise(F.lit(0)),
)
link_order_to_delivery = (
    vbfa
    .filter((F.col("vbtyp_v") == ORDER) & (F.col("vbtyp_n") == DELIVERY))
    .select(
        F.col("vbelv").alias("order_vbeln"),
        F.col("posnv").alias("order_posnr"),
        F.col("vbeln").alias("delivery_vbeln"),
        F.col("posnn").alias("delivery_posnr"),
        F.col("rfmng").alias("delivered_qty"),
    )
    .join(vbap.select("order_vbeln", "order_posnr", "unit_price"), on=["order_vbeln", "order_posnr"], how="left")
    .withColumn("delivered_value", F.col("unit_price") * F.col("delivered_qty"))
    .drop("unit_price")
)

link_delivery_to_billing = (
    vbfa
    .filter((F.col("vbtyp_v") == DELIVERY) & (F.col("vbtyp_n") == INVOICE))
    .withColumn("signed_value", F.when(F.col("plmin") == "-", -F.col("rfwrt")).otherwise(F.col("rfwrt")))
    .select(
        F.col("vbelv").alias("delivery_vbeln"),
        F.col("posnv").alias("delivery_posnr"),
        F.col("vbeln").alias("billing_vbeln"),
        F.col("posnn").alias("billing_posnr"),
        F.col("signed_value").alias("billed_value"),
        F.col("rfmng").alias("billed_qty"),
    )
)

link_order_to_billing_direct = (
    vbfa
    .filter((F.col("vbtyp_v") == ORDER) & (F.col("vbtyp_n") == INVOICE))
    .withColumn("signed_value", F.when(F.col("plmin") == "-", -F.col("rfwrt")).otherwise(F.col("rfwrt")))
    .select(
        F.col("vbelv").alias("order_vbeln"),
        F.col("posnv").alias("order_posnr"),
        F.col("vbeln").alias("direct_billing_vbeln"),
        F.col("posnn").alias("direct_billing_posnr"),
        F.col("signed_value").alias("direct_billed_value"),
    )
)
 

# COMMAND ----------

fact_order_to_cash_via_delivery = (
    vbap
    .join(link_order_to_delivery, on=["order_vbeln", "order_posnr"], how="left")
    .join(link_delivery_to_billing, on=["delivery_vbeln", "delivery_posnr"], how="left")
    .withColumn("flow_type", F.lit("VIA_DELIVERY"))
)
# Billing documents already reached through the delivery chain, per order
# line — VBFA sometimes records BOTH a delivery->invoice link AND a direct
# order->invoice link for the SAME physical invoice (confirmed in this
# dataset: e.g. billing doc 0090007751/000020 shows up under both flow
# types for order 0000005602/000020). Without this exclusion, that one
# real invoice gets double-listed, which would double-count its value in
# any downstream SUM(billed_value) done directly on this fact table.
already_covered_billing = (
    fact_order_to_cash_via_delivery
    .filter(F.col("billing_vbeln").isNotNull())
    .select("order_vbeln", "order_posnr", "billing_vbeln", "billing_posnr")
    .distinct()
)
 
link_order_to_billing_direct_deduped = (
    link_order_to_billing_direct
    .withColumnRenamed("direct_billing_vbeln", "billing_vbeln")
    .withColumnRenamed("direct_billing_posnr", "billing_posnr")
    .join(
        already_covered_billing.withColumn("_covered", F.lit(True)),
        on=["order_vbeln", "order_posnr", "billing_vbeln", "billing_posnr"],
        how="left",
    )
    .filter(F.col("_covered").isNull())   # keep only invoices NOT already seen via delivery
    .drop("_covered")
    .withColumnRenamed("billing_vbeln", "direct_billing_vbeln")
    .withColumnRenamed("billing_posnr", "direct_billing_posnr")
)
 
# Direct order->billing rows (skips delivery) — INNER join, since this only
# adds rows for order lines that genuinely have a direct link not already
# covered above; still no cartesian multiplication, and now no double-listing.
fact_order_to_cash_direct = (
    vbap
    .join(link_order_to_billing_direct_deduped, on=["order_vbeln", "order_posnr"], how="inner")
    .withColumn("delivery_vbeln", F.lit(None).cast("string"))
    .withColumn("delivery_posnr", F.lit(None).cast("string"))
    .withColumn("delivered_qty", F.lit(None).cast("decimal(13,3)"))
    .withColumn("delivered_value", F.lit(None).cast("decimal(15,2)"))
    .withColumnRenamed("direct_billing_vbeln", "billing_vbeln")
    .withColumnRenamed("direct_billing_posnr", "billing_posnr")
    .withColumnRenamed("direct_billed_value", "billed_value")
    .withColumn("billed_qty", F.lit(None).cast("decimal(13,3)"))
    .withColumn("flow_type", F.lit("DIRECT_BILLING"))
)
 
fact_order_to_cash = fact_order_to_cash_via_delivery.unionByName(
    fact_order_to_cash_direct, allowMissingColumns=True
)

fact_order_to_cash.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true").saveAsTable(f"{SILVER_SCHEMA}.fact_order_to_cash1")
 

# COMMAND ----------

# =============================================================================
# US-9.2.1 — Derive Open Order Value (ordered minus delivered).
# Aggregate delivered value per order line from the order->delivery link
# (summing handles an order line split across multiple deliveries), then
# subtract from the order's own value.
# =============================================================================
 
delivered_per_order = (
    link_order_to_delivery
    .groupBy("order_vbeln", "order_posnr")
    .agg(F.sum("delivered_value").alias("total_delivered_value"))
)
 
open_order_value = (
    vbap
    .join(delivered_per_order, on=["order_vbeln", "order_posnr"], how="left")
    .withColumn("total_delivered_value", F.coalesce(F.col("total_delivered_value"), F.lit(0)))
    .withColumn("open_order_value", F.col("order_value") - F.col("total_delivered_value"))
    .select("order_vbeln", "order_posnr", "order_matnr", "order_value",
            "total_delivered_value", "open_order_value")
)
 
(open_order_value.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.open_order_value"))
 
print("Open Order Value summary:")
open_order_value.agg(
    F.sum("order_value").alias("total_ordered"),
    F.sum("total_delivered_value").alias("total_delivered"),
    F.sum("open_order_value").alias("total_open"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GOLD TABLES

# COMMAND ----------

dim_customer_lookup = dim_customer.filter(F.col(CONFIG["CUSTOMER_IS_CURRENT_COL"]) == True).\
    select(F.col("Customer_ID").alias(CONFIG["FACT_CUSTOMER_COL"]),F.col("Customer_SK"))

dim_material_lookup = dim_material.filter(F.col(CONFIG["MATERIAL_IS_CURRENT_COL"]) == True).\
     select(F.col("Material_ID").alias(CONFIG["FACT_MATERIAL_COL"]), F.col("Material_SK"))

dim_sales_org_lookup=dim_sales_organization.select(
        F.col("vkorg").alias(CONFIG["FACT_SALES_ORG_COL"]),
        F.col("sales_org_sk")
    )
    
dim_date_lookup = dim_date.select(
    F.col(CONFIG["DATE_DIM_KEY"]).alias("order_date_sk_lookup"),
    F.col(CONFIG["DATE_DIM_KEY"]).alias("order_date_sk")
)
dim_currency_lookup = dim_currency_rate.select(
    F.col("currency_rate_sk"),
    F.col("from_currency"),
    F.col("to_currency"),
    F.col("rate_date_sk"),
    F.col("exchange_rate").alias("currency_exchange_rate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SALES ORDER

# COMMAND ----------

step1 = fact_sales_order.join(F.broadcast(dim_customer_lookup), on=CONFIG["FACT_CUSTOMER_COL"], how="left")
step2 = step1.join(F.broadcast(dim_material_lookup), on=CONFIG["FACT_MATERIAL_COL"], how="left")
step3 = step2.join(F.broadcast(dim_sales_org_lookup), on=CONFIG["FACT_SALES_ORG_COL"], how="left")
step3_dated = step3.withColumn(
    "order_date_sk_lookup",
    F.date_format(F.col(CONFIG["FACT_DATE_COL"]), "yyyyMMdd").cast("int")
)
step4 = step3_dated.join(F.broadcast(dim_date_lookup), on="order_date_sk_lookup", how="left").drop("order_date_sk_lookup")

step5 = step4.alias("fact").join(F.broadcast(dim_currency_lookup).alias("rate"),
    (
        (F.col("rate.from_currency") == F.col("fact.source_currency")) &
        (F.col("rate.to_currency") == F.lit("USD")) &
        (F.col("rate.rate_date_sk") == F.date_format(F.col("fact.rate_date"),"yyyyMMdd").cast("int"))
    ),
    how="left"
)

fact_sales_order_with_sk = step5.select(
    "sales_order",
    "sales_order_item",
    "customer_sk",
    "material_sk",
    "sales_org_sk",
    "order_date_sk",
    "currency_rate_sk",
    "order_quantity",
    "item_net_value",
    #"source_currency",
    #F.col("currency_rate_date").alias("rate_date"),
    #F.col("currency_exchange_rate").alias("exchange_rate")
    )

fact_sales_order_with_sk.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{GOLD_SCHEMA}.fact_sales_order_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC ####DELIVERY

# COMMAND ----------

fact_j1 = (
    fact_delivery_gold.alias("f")
    .join(
        F.broadcast(dim_material).alias("d1"),
        on=(
            (F.col("f.material") == F.col("d1.Material_ID"))
            & (F.col("f.delivery_created_date") >= F.col("d1.Start_Date"))
            & (
                (F.col("f.delivery_created_date") < F.col("d1.End_Date"))
                | F.col("d1.End_Date").isNull()
            )
        ),
        how="left",
    )
    .select("f.*", F.col("d1.Material_SK").alias("material_sk"))
)

fact_j2 = (
    fact_j1.alias("f")
    .join(
        F.broadcast(dim_date).alias("d2"),
        on=F.col("f.order_created_date") == F.col("d2.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d2.date_sk").alias("order_created_date_sk"))
)
fact_j3 = (
    fact_j2.alias("f")
    .join(
        F.broadcast(dim_date).alias("d3"),
        on=F.col("f.delivery_created_date") == F.col("d3.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d3.date_sk").alias("delivery_created_date_sk"))
)
fact_j4 = (
    fact_j3.alias("f")
    .join(
        F.broadcast(dim_date).alias("d4"),
        on=F.col("f.planned_goods_issue_date") == F.col("d4.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d4.date_sk").alias("planned_goods_issue_date_sk"))
)
fact_j5 = (
    fact_j4.alias("f")
    .join(
        F.broadcast(dim_date).alias("d5"),
        on=F.col("f.actual_goods_issue_date") == F.col("d5.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d5.date_sk").alias("actual_goods_issue_date_sk"))
)
fact_j6 = (
    fact_j5.alias("f")
    .join(
        F.broadcast(dim_date).alias("d6"),
        on=F.col("f.planned_delivery_date") == F.col("d6.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d6.date_sk").alias("planned_delivery_date_sk"))
)
fact_j7 = (
    fact_j6.alias("f")
    .join(
        F.broadcast(dim_date).alias("d7"),
        on=F.col("f.delivery_created_date") == F.col("d7.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d7.date_sk").alias("delivery_date_sk"))
)

fact_final = fact_j7.drop(
    "material",
    "order_created_date", "delivery_created_date", "planned_goods_issue_date",
    "actual_goods_issue_date", "planned_delivery_date", "delivery_created_date",
)

fact_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{GOLD_SCHEMA}.fact_delivery_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC ####BILLING

# COMMAND ----------

fact_with_sold_to = (
    fact_billing_gold.alias("f")
    .join(
        F.broadcast(dim_customer).alias("d_sold"),
        on=(
            (F.col("f.sold_to_customer") == F.col("d_sold.Customer_ID"))
            & (F.col("f.billing_date") >= F.col("d_sold.Start_Date"))
            & (F.col("d_sold.End_Date").isNull()
                |(F.col("f.billing_date") <  F.col("d_sold.End_Date")))
        ),
        how="left",
    )
    .select("f.*", F.col("d_sold.Customer_SK").alias("soldto_customer_sk"))
)

fact_with_both_customers = (
    fact_with_sold_to.alias("f")
    .join(
        F.broadcast(dim_customer).alias("d_payer"),
        on=(
            (F.col("f.payer_customer") == F.col("d_payer.Customer_ID"))
            & (F.col("f.billing_date") >= F.col("d_payer.Start_Date"))
            & (F.col("f.billing_date") <  F.col("d_payer.End_Date"))
        ),
        how="left",
    )
    .select("f.*", F.col("d_payer.Customer_SK").alias("payer_customer_sk"))
)

fact_resolved = fact_with_both_customers
#.withColumn("soldto_customer_sk", F.coalesce(F.col("soldto_customer_sk"), F.lit(UNKNOWN_CUSTOMER_SK))
#).withColumn("payer_customer_sk", F.coalesce(F.col("payer_customer_sk"), F.lit(UNKNOWN_CUSTOMER_SK)))

# COMMAND ----------

step1 = fact_resolved.drop("sold_to_customer", "payer_customer")
fact_j1 = (
    fact_billing_gold.alias("f")
    .join(
        F.broadcast(dim_customer).alias("d1"),
        on=(
            (F.col("f.sold_to_customer") == F.col("d1.Customer_ID"))
            & (F.col("f.billing_date") >= F.col("d1.Start_Date"))
            & (
                (F.col("f.billing_date") < F.col("d1.End_Date"))
                | F.col("d1.End_Date").isNull()
            )
        ),
        how="left",
    )
    .select("f.*", F.col("d1.Customer_SK").alias("soldto_customer_sk"))
)
fact_j1 = (
    fact_j1.alias("f")
    .join(
        F.broadcast(dim_customer).alias("d2"),
        on=(
            (F.col("f.payer_customer") == F.col("d2.Customer_ID"))
            & (F.col("f.billing_date") >= F.col("d2.Start_Date"))
            & (
                (F.col("f.billing_date") < F.col("d2.End_Date"))
                | F.col("d2.End_Date").isNull()
            )
        ),
        how="left",
    )
    .select("f.*", F.col("d2.Customer_SK").alias("payer_customer_sk"))
)

fact_j2 = (
    fact_j1.alias("f")
    .join(
        F.broadcast(dim_material).alias("d3"),
        on=(
            (F.col("f.material") == F.col("d3.Material_ID"))
            & (F.col("f.billing_date") >= F.col("d3.Start_Date"))
            & (
                (F.col("f.billing_date") < F.col("d3.End_Date"))
                | F.col("d3.End_Date").isNull()
            )
        ),
        how="left",
    )
    .select("f.*", F.col("d3.Material_SK").alias("material_sk"))
)

fact_j3 = (
    fact_j2.alias("f")
    .join(
        F.broadcast(dim_sales_organization).alias("d4"),
        on=F.col("f.sales_org_sk") == F.col("d4.sales_org_sk"),
        how="left",
    )
    .select("f.*", F.col("d4.sales_org_sk").alias("sales_org_SK_"))
)

fact_j4 = (
    fact_j3.alias("f")
    .join(
        F.broadcast(dim_date).alias("d5"),
        on=F.col("f.billing_date") == F.col("d5.calendar_date"),
        how="left",
    )
    .select("f.*", F.col("d5.date_sk").alias("billing_date_sk"))
)

fact_j5 = (
    fact_j4.alias("f")
    .join(
        F.broadcast(dim_currency_rate).alias("d6"),
        on=(
            (F.col("f.source_currency") == F.col("d6.from_currency"))
            & (F.col("f.group_currency") == F.col("d6.to_currency"))
            & ( F.date_format(F.col("f.rate_date"),"yyyyMMdd").cast("int") == F.col("d6.rate_date_sk"))
        ),
        how="left",
    )
    .select("f.*", F.col("d6.currency_rate_sk").alias("currency_rate_sk"))
)

fact_final = (
    fact_j5
    .drop(
        "sold_to_customer", "payer_customer", "material",
        "source_currency", "group_currency", "rate_date", "sales_org_sk",
        "billing_date", "exchange_rate"
    )
    .withColumnRenamed("sales_org_sk_resolved", "sales_org_sk")
)

fact_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{GOLD_SCHEMA}.fact_billing_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC ####ORDER TO CASH

# COMMAND ----------

dim_material_lookup = dim_material.select(
    F.col("Material_SK"), F.col("Material_ID")
)

fact_j1 = (
    fact_order_to_cash.alias("f")
    .join(
        F.broadcast(dim_material_lookup).alias("d1"),
        on=F.col("f.order_matnr") == F.col("d1.Material_ID"),
        how="left",
    )
    .select("f.*", F.col("d1.Material_SK").alias("material_sk"))
)

sales_order_lookup = (
    fact_sales_order_gold_final
    .select(
        F.col("sales_order").alias("order_vbeln"),
        F.col("sales_order_item").alias("order_posnr"),
        F.col("customer_sk"),
        F.col("sales_org_sk"),
        F.col("order_date_sk"),
        F.col("material_sk"),
    )
)
 
fact_otc_enriched = fact_order_to_cash.join(
    sales_order_lookup,
    on=["order_vbeln", "order_posnr"],
    how="left",
)

billing_lookup = (
    fact_billing_gold_final
    .select(
        F.col("billing_number").alias("billing_vbeln"),
        F.col("billing_item").alias("billing_posnr"),
        F.col("billing_date_sk"),
        F.col("currency_rate_sk").alias("billing_currency_rate_sk"),
    )
)
 
fact_otc_enriched = fact_otc_enriched.join(
    billing_lookup,
    on=["billing_vbeln", "billing_posnr"],
    how="left",
)
 

# COMMAND ----------

fact_otc_enriched = fact_otc_enriched.withColumnRenamed("billing_currency_rate_sk", "currency_rate_sk")

fact_otc_final = (
    fact_otc_enriched
    .withColumn("delivered_value_calc", F.coalesce(F.col("delivered_value"), F.lit(0)))
    .withColumn("billed_value_calc", F.coalesce(F.col("billed_value"), F.lit(0)))
    .withColumn(
        "open_order_value",
        F.greatest(F.col("order_value") - F.col("delivered_value_calc"), F.lit(0))
    )
    .withColumn(
        "unbilled_delivery_value",
        F.greatest(F.col("delivered_value_calc") - F.col("billed_value_calc"), F.lit(0))
    )
    .drop("delivered_value_calc", "billed_value_calc")
)

fact_otc_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{GOLD_SCHEMA}.fact_order_to_cash_gold")