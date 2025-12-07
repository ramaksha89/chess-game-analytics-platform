# Databricks notebook source
# MAGIC %md
# MAGIC # Player Rating SCD Type 2 Transform
# MAGIC 
# MAGIC This notebook implements SCD Type 2 for player ratings from chess.com and lichess APIs.
# MAGIC We track historical changes in player ratings over time to enable trend analysis.
# MAGIC 
# MAGIC **Input**: Bronze layer (raw API data)
# MAGIC **Output**: Silver layer (SCD Type 2 dimension table)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set up paths - using Delta Lake format
bronze_path = "/mnt/chess-analytics/bronze/player_stats/"
silver_path = "/mnt/chess-analytics/silver/dim_player_rating/"

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Read Bronze Layer Data

# COMMAND ----------

# Read the latest snapshot from bronze
# TODO: parameterize this to read incremental data only
df_bronze = spark.read.format("delta").load(bronze_path)

# Add ingestion metadata
df_bronze = df_bronze.withColumn("ingestion_ts", F.current_timestamp())

print(f"Bronze records loaded: {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformations

# COMMAND ----------

# Clean and standardize the data
df_clean = df_bronze.select(
    F.col("username").alias("player_username"),
    F.col("platform"),  # chess.com or lichess
    F.col("rating_blitz").cast("int"),
    F.col("rating_rapid").cast("int"),
    F.col("rating_bullet").cast("int"),
    F.col("rating_classical").cast("int"),
    F.col("games_played").cast("int"),
    F.col("wins").cast("int"),
    F.col("losses").cast("int"),
    F.col("draws").cast("int"),
    F.col("data_date").cast("date"),
    F.col("ingestion_ts")
)

# Calculate derived metrics
df_clean = df_clean.withColumn(
    "win_rate", 
    F.when(F.col("games_played") > 0, 
           F.col("wins") / F.col("games_played") * 100
    ).otherwise(0)
)

# Add business key (natural key)
df_clean = df_clean.withColumn(
    "player_key",
    F.concat(F.col("player_username"), F.lit("_"), F.col("platform"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Implementation
# MAGIC 
# MAGIC We implement SCD Type 2 to track rating changes over time.
# MAGIC - `is_current`: Flag for current record (Y/N)
# MAGIC - `effective_date`: When this version became active  
# MAGIC - `end_date`: When this version expired (null for current)
# MAGIC - `record_hash`: Hash of attribute values to detect changes

# COMMAND ----------

# Create hash of the attributes we want to track for changes
# This makes change detection more efficient than comparing all columns
attribute_cols = ["rating_blitz", "rating_rapid", "rating_bullet", 
                  "rating_classical", "games_played", "wins", "losses", "draws"]

df_clean = df_clean.withColumn(
    "record_hash",
    F.sha2(F.concat_ws("|", *attribute_cols), 256)
)

# COMMAND ----------

# Check if silver table exists
try:
    df_existing = spark.read.format("delta").load(silver_path)
    table_exists = True
    print("Existing silver table found")
except Exception as e:
    table_exists = False
    print("No existing silver table - this is initial load")

# COMMAND ----------

if not table_exists:
    # Initial load - all records are current
    df_scd = df_clean.select(
        F.monotonically_increasing_id().alias("surrogate_key"),
        "player_key",
        "player_username",
        "platform",
        "rating_blitz",
        "rating_rapid",
        "rating_bullet",
        "rating_classical",
        "games_played",
        "wins",
        "losses",
        "draws",
        "win_rate",
        "record_hash",
        F.col("data_date").alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit("Y").alias("is_current"),
        "ingestion_ts"
    )
    
    # Write initial load
    df_scd.write.format("delta").mode("overwrite").save(silver_path)
    print(f"Initial load complete: {df_scd.count()} records written")
    
else:
    # Incremental load - need to apply SCD Type 2 logic
    
    # Get only current records from existing table
    df_current = df_existing.filter(F.col("is_current") == "Y")
    
    # Join new data with current records to identify changes
    df_compare = df_clean.alias("new").join(
        df_current.alias("curr"),
        F.col("new.player_key") == F.col("curr.player_key"),
        "left"
    )
    
    # Identify record types:
    # 1. New players (no match in current)
    # 2. Changed records (hash mismatch)
    # 3. Unchanged records (hash match - skip these)
    
    df_compare = df_compare.withColumn(
        "change_type",
        F.when(F.col("curr.player_key").isNull(), "NEW")
         .when(F.col("new.record_hash") != F.col("curr.record_hash"), "CHANGED")
         .otherwise("UNCHANGED")
    )
    
    # Filter for records that need processing
    df_process = df_compare.filter(F.col("change_type").isin(["NEW", "CHANGED"]))
    
    # For CHANGED records, we need to:
    # 1. Close the old record (set end_date and is_current='N')
    # 2. Insert the new record as current
    
    # Get records to close (existing records that changed)
    df_to_close = df_process.filter(F.col("change_type") == "CHANGED").select(
        F.col("curr.surrogate_key"),
        F.col("curr.player_key"),
        F.col("curr.player_username"),
        F.col("curr.platform"),
        F.col("curr.rating_blitz"),
        F.col("curr.rating_rapid"),
        F.col("curr.rating_bullet"),
        F.col("curr.rating_classical"),
        F.col("curr.games_played"),
        F.col("curr.wins"),
        F.col("curr.losses"),
        F.col("curr.draws"),
        F.col("curr.win_rate"),
        F.col("curr.record_hash"),
        F.col("curr.effective_date"),
        F.col("new.data_date").alias("end_date"),  # End date is when new version starts
        F.lit("N").alias("is_current"),
        F.col("curr.ingestion_ts")
    )
    
    # Get new records to insert (both NEW and CHANGED)
    df_to_insert = df_process.select(
        F.monotonically_increasing_id().alias("surrogate_key"),
        F.col("new.player_key"),
        F.col("new.player_username"),
        F.col("new.platform"),
        F.col("new.rating_blitz"),
        F.col("new.rating_rapid"),
        F.col("new.rating_bullet"),
        F.col("new.rating_classical"),
        F.col("new.games_played"),
        F.col("new.wins"),
        F.col("new.losses"),
        F.col("new.draws"),
        F.col("new.win_rate"),
        F.col("new.record_hash"),
        F.col("new.data_date").alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit("Y").alias("is_current"),
        F.col("new.ingestion_ts")
    )
    
    # Combine closed records and new records
    df_updates = df_to_close.unionByName(df_to_insert)
    
    # Merge into silver table using Delta Lake MERGE
    from delta.tables import DeltaTable
    
    delta_table = DeltaTable.forPath(spark, silver_path)
    
    # This is a bit tricky - we need to update existing records and insert new ones
    # Using Delta Lake's merge capability
    delta_table.alias("target").merge(
        df_updates.alias("source"),
        "target.surrogate_key = source.surrogate_key"
    ).whenMatchedUpdate(
        set = {
            "end_date": "source.end_date",
            "is_current": "source.is_current"
        }
    ).whenNotMatchedInsertAll()
    .execute()
    
    print(f"SCD Type 2 merge complete")
    print(f"Records closed: {df_to_close.count()}")
    print(f"New records inserted: {df_to_insert.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation & Data Quality Checks

# COMMAND ----------

# Read final silver table
df_final = spark.read.format("delta").load(silver_path)

# Quality checks
print("=== Data Quality Checks ===")
print(f"Total records in silver: {df_final.count()}")
print(f"Current records: {df_final.filter(F.col('is_current') == 'Y').count()}")
print(f"Historical records: {df_final.filter(F.col('is_current') == 'N').count()}")

# Check for duplicates in current records (should be zero)
duplicate_check = df_final.filter(F.col("is_current") == "Y") \
    .groupBy("player_key").count() \
    .filter(F.col("count") > 1)

if duplicate_check.count() > 0:
    print("WARNING: Duplicate current records found!")
    duplicate_check.show()
else:
    print("✓ No duplicate current records")

# Show sample data
print("\n=== Sample Data ===")
df_final.filter(F.col("is_current") == "Y").orderBy(F.desc("rating_rapid")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC - Schedule this notebook to run daily via Databricks Workflows
# MAGIC - Add alerting for data quality issues
# MAGIC - Implement data lineage tracking
# MAGIC - Create views for downstream consumption
