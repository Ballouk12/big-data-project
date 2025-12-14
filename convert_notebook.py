
import json
import re

zeppelin_path = r"d:\Users\HP\Desktop\big-data (2)\big-data\zeppelin\notebooks\PRÉTRAITEMENT  DATASET ML_2MD67YCQM.zpln"
ipynb_path = r"d:\Users\HP\Desktop\big-data (2)\big-data\zeppelin\notebooks\PRETRAITEMENT_DATASET_ML.ipynb"

# Load Zeppelin Notebook
with open(zeppelin_path, 'r', encoding='utf-8') as f:
    z_content = json.load(f)

# Fixes to apply
def fix_code(code):
    # Fix Section 4
    if "SECTION 4: CONVERSION DES TYPES" in code:
        code = """%spark.pyspark
print("="*80)
print("SECTION 4: CONVERSION DES TYPES")
print("="*80)

from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, trim, when, to_timestamp

df_converted = df_clean

# 1. Nettoyer chaînes "null"
for c in df_converted.columns:
    if str(df_converted.schema[c].dataType) == "StringType":
        df_converted = df_converted.withColumn(c, when(trim(col(c))=="null", None).otherwise(col(c)))
print("✅ Nettoyage 'null' terminé")

# 2. Conversion colonnes numériques (Weather Data)
# Double
for c in ["data_temperature", "data_windspeed"]:
    if c in df_converted.columns:
        df_converted = df_converted.withColumn(c, col(c).cast(DoubleType()))
        print(f"✅ {c} → Double")

# Integer
for c in ["data_weathercode", "data_winddirection"]:
    if c in df_converted.columns:
        df_converted = df_converted.withColumn(c, col(c).cast(IntegerType()))
        print(f"✅ {c} → Integer")

# 3. Conversion timestamps
if "data_time" in df_converted.columns:
    df_converted = df_converted.withColumn("data_timestamp", to_timestamp(col("data_time")))
    print("✅ Conversion timestamp 'data_time' → 'data_timestamp' terminée")

df_converted.printSchema()
df_converted.createOrReplaceTempView("tfl_converted_types")
"""
    
    # Fix Section 6
    if "SECTION 6: CREATION DES FEATURES TEMPORLLES" in code:
        code = """%spark.pyspark
print("="*80)
print("SECTION 6: CREATION DES FEATURES TEMPORELLES")
print("="*80)

from pyspark.sql.functions import hour, minute, second, dayofmonth, month, year, dayofweek

df_temp = df_converted
df_features = df_temp

# Composantes de timestamp
# Using 'data_timestamp'
for name, func in [("hour", hour("data_timestamp")),
                   ("minute", minute("data_timestamp")),
                   ("second", second("data_timestamp")),
                   ("day", dayofmonth("data_timestamp")),
                   ("month", month("data_timestamp")),
                   ("year", year("data_timestamp")),
                   ("day_of_week", dayofweek("data_timestamp"))]:
    df_features = df_features.withColumn(f"ts_{name}", func)

# Catégories temporelles
df_features = df_features.withColumn("time_of_day",
                                     when((col("ts_hour")>=0)&(col("ts_hour")<6),"night")
                                     .when((col("ts_hour")>=6)&(col("ts_hour")<12),"morning")
                                     .when((col("ts_hour")>=12)&(col("ts_hour")<18),"afternoon")
                                     .otherwise("evening"))

# Weekend: 1=Sunday, 7=Saturday
df_features = df_features.withColumn("is_weekend",
                                     when((col("ts_day_of_week")==1)|(col("ts_day_of_week")==7),1).otherwise(0))

# Peak hour (7-10, 16-19)
df_features = df_features.withColumn("is_peak_hour",
                                     when((col("ts_hour")>=7)&(col("ts_hour")<10),1)
                                     .when((col("ts_hour")>=16)&(col("ts_hour")<19),1).otherwise(0))

df_features.createOrReplaceTempView("tfl_with_temporal_features")
print("✅ Features temporelles créées")

# Aperçu
df_features.select("data_timestamp","ts_hour","ts_day_of_week",
                   "time_of_day","is_weekend","is_peak_hour").show(10)
"""

    # Cleanup Zeppelin magic
    code = code.replace("%spark.pyspark\r\n", "").replace("%spark.pyspark\n", "")
    return code

# Convert to Cells
cells = []
for p in z_content.get('paragraphs', []):
    if 'text' in p:
        code_content = fix_code(p['text'])
        if code_content.strip():
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": code_content.splitlines(True)
            })

# Create IPYNB structure
notebook = {
 "cells": cells,
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}

# Save
with open(ipynb_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1)

print(f"Successfully converted to {ipynb_path}")
