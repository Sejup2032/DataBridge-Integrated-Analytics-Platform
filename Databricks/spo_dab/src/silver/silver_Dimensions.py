# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import os
import sys

project_path = os.path.join(os.getcwd(),'..','..')

sys.path.append(project_path)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md 
# MAGIC # DimUser
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader

# COMMAND ----------

# 2. Setup the Auto Loader (The Read side)
df_user = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .option("cloudFiles.schemaLocation", "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimUser/metadata/schema")
           .load("abfss://bronze@spoazuredatalakestorage.dfs.core.windows.net/DimUser"))


# COMMAND ----------

checkpoint_To_display = "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimUser/metadata/display_logs"
from utils.transformations import reusable
df_user_obj = reusable()

# 2. Apply the drop and RE-ASSIGN to df_art
df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
df_final = df_user.withColumn("user_name",upper(col("user_name")))
display(df_final,
        checkpointLocation = checkpoint_To_display, 
        outputMode = "append")


# COMMAND ----------

df = df_final.writeStream.format("delta")\
       .outputMode("append")\
       .option("checkpointLocation","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimUser/metadata/write_log")\
       .trigger(once=True)\
       .option("path","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimUser/Data")\
       .toTable("spo_cat.silver.DimUser")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DimArtist

# COMMAND ----------

# 2. Setup the Auto Loader (The Read side)
df_art = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .option("cloudFiles.schemaLocation", "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimArtist/metadata/schema")
           .load("abfss://bronze@spoazuredatalakestorage.dfs.core.windows.net/DimArtist"))


# COMMAND ----------

checkpoint_To_display = "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimArtist/metadata/display_log"
# df_final = df_user.withColumn("user_name",upper(col("user_name")))
display(df_art,
        checkpointLocation = checkpoint_To_display, 
        outputMode = "append")

        

# COMMAND ----------

# 1. Import and Initialize
from utils.transformations import reusable
df_art_obj = reusable()

# 2. Apply the drop and RE-ASSIGN to df_art
df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])

# COMMAND ----------

df_art = df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimArtist/metadata/write_log")\
    .option("path","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimArtist/data")\
    .trigger(once=True)\
    .toTable("spo_cat.silver.DimArtist")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DimTrack

# COMMAND ----------

#Setup the Auto Loader (The Read side)
df_track = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .option("cloudFiles.schemaLocation", "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimTrack/metadata/schema")
           .load("abfss://bronze@spoazuredatalakestorage.dfs.core.windows.net/DimTrack"))


# COMMAND ----------

checkpoint_display = "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimTrack/metadata/display_logs"
# df_final = df_user.withColumn("user_name",upper(col("user_name")))
display(df_track,
        checkpointLocation = checkpoint_display, 
        outputMode = "append")


# COMMAND ----------

df_track = df_track.withColumn("duration_sec", when(col('duration_sec')<150,"low")\
                                              .when(col('duration_sec')<300,"medium")\
                                              .otherwise("high")        
    )
df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))
df_track = reusable().dropColumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track = df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimTrack/metadata/trans_logs")\
    .option("path","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimTrack/data")\
    .trigger(once=True)\
    .toTable("spo_cat.silver.DimTrack")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DimDate

# COMMAND ----------

#Setup the Auto Loader (The Read side)
df_date = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .option("cloudFiles.schemaLocation", "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimDate/metadata/schema")
           .load("abfss://bronze@spoazuredatalakestorage.dfs.core.windows.net/DimDate"))

# COMMAND ----------

checkpoint_display = "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimDate/metadata/display_logs"
# df_final = df_user.withColumn("user_name",upper(col("user_name")))
display(df_date,
        checkpointLocation = checkpoint_display, 
        outputMode = "append")


# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_Data'])

# COMMAND ----------

df_date = df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimDate/metadata/trans_logs")\
    .trigger(once=True)\
    .option("path","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/DimDate/data")\
    .toTable("spo_cat.silver.DimDate")


# COMMAND ----------

# MAGIC %md
# MAGIC ## FactStream

# COMMAND ----------

#Setup the Auto Loader (The Read side)
df_fact = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .option("cloudFiles.schemaLocation", "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/FactStream/metadata/schema")
           .load("abfss://bronze@spoazuredatalakestorage.dfs.core.windows.net/FactStream"))

# COMMAND ----------

checkpoint_display = "abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/FactStream/metadata/display_logs"
# df_final = df_user.withColumn("user_name",upper(col("user_name")))
display(df_fact,
        checkpointLocation = checkpoint_display, 
        outputMode = "append")


# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact = df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/FactStream/metadata/trans_logs")\
    .trigger(once=True)\
    .option("path","abfss://silver@spoazuredatalakestorage.dfs.core.windows.net/FactStream/data")\
    .toTable("spo_cat.silver.FactStream")


# COMMAND ----------

