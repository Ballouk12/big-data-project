
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, when, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler

def test_feature_engineering():
    spark = SparkSession.builder \
        .appName("DebugFeatures") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Sample Data (like Open-Meteo)
    # Time usually lacks seconds: "2023-10-27T10:00"
    data = [
        (15.0, 10.0, 180.0, 3.0, "2023-12-15T14:30", 1700000000.0), # Afternoon
        (10.0, 5.0, 100.0, 1.0, "2023-12-15T09:00", 1700000000.0),  # Morning
        (5.0, 20.0, 0.0, 0.0, "2023-12-15T20:00", 1700000000.0)    # Evening
    ]
    
    schema = StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("winddirection", DoubleType()),
        StructField("weathercode", DoubleType()),
        StructField("time", StringType()),
        StructField("ingest_time", DoubleType())
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("--- RAW DATA ---")
    df.show(truncate=False)

    # Apply Logic
    df_features = df.withColumn("data_timestamp", to_timestamp(col("time")))
    
    print("--- AFTER TIMESTAMP PARSING ---")
    df_features.select("time", "data_timestamp").show(truncate=False)
    
    # Rename
    df_features = df_features \
        .withColumnRenamed("windspeed", "data_windspeed") \
        .withColumnRenamed("weathercode", "data_weathercode") \
        .withColumnRenamed("winddirection", "data_winddirection")
        
    # Features
    df_features = df_features.withColumn("ts_hour", hour("data_timestamp"))

    # Check ts_hour
    print("--- HOUR EXTRACTION ---")
    df_features.select("time", "data_timestamp", "ts_hour").show()
    
    # OHE
    df_features = df_features \
        .withColumn("is_afternoon", when((col("ts_hour")>=12)&(col("ts_hour")<18), 1.0).otherwise(0.0)) \
        .withColumn("is_evening", when(col("ts_hour")>=18, 1.0).otherwise(0.0)) \
        .withColumn("is_morning", when((col("ts_hour")>=6)&(col("ts_hour")<12), 1.0).otherwise(0.0))

    feature_cols = [
        "data_windspeed", 
        "data_weathercode", 
        "data_winddirection", 
        "ts_hour", 
        "is_afternoon",
        "is_evening",
        "is_morning"
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    try:
        df_final = assembler.transform(df_features)
        print("--- FINAL FEATURES ---")
        df_final.select("features").show(truncate=False)
    except Exception as e:
        print(f"ERROR in Assembler: {e}")

if __name__ == "__main__":
    test_feature_engineering()
