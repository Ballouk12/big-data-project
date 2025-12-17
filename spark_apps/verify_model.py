
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel

def verify_model():
    spark = SparkSession.builder \
        .appName("VerifyModel") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Sample Data (Varying conditions)
    # 1. Afternoon, 15C
    # 2. Morning, 10C
    # 3. Evening, 5C
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
    
    # Feature Engineering (Same as inference)
    df_features = df.withColumn("data_timestamp", to_timestamp(col("time"))) \
        .withColumnRenamed("windspeed", "data_windspeed") \
        .withColumnRenamed("weathercode", "data_weathercode") \
        .withColumnRenamed("winddirection", "data_winddirection") \
        .withColumn("ts_hour", hour("data_timestamp")) \
        .withColumn("is_weekend", when((col("data_timestamp").cast("string").contains("Sun")) | (col("data_timestamp").cast("string").contains("Sat")), 1.0).otherwise(0.0)) \
        .withColumn("is_peak_hour", when((col("ts_hour")>=7)&(col("ts_hour")<10), 1.0)
                                         .when((col("ts_hour")>=16)&(col("ts_hour")<19), 1.0).otherwise(0.0)) \
        .withColumn("is_afternoon", when((col("ts_hour")>=12)&(col("ts_hour")<18), 1.0).otherwise(0.0)) \
        .withColumn("is_evening", when(col("ts_hour")>=18, 1.0).otherwise(0.0)) \
        .withColumn("is_morning", when((col("ts_hour")>=6)&(col("ts_hour")<12), 1.0).otherwise(0.0))

    feature_cols = [
        "data_windspeed", 
        "data_weathercode", 
        "data_winddirection", 
        "ts_hour", 
        "is_weekend", 
        "is_peak_hour",
        "is_afternoon",
        "is_evening",
        "is_morning"
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_final = assembler.transform(df_features)

    # Load Model
    MODEL_PATH = "hdfs://namenode-bd:9000/user/zeppelin/weather_rf_model_v2"
    print(f"📂 Loading Model from {MODEL_PATH}...")
    try:
        model = RandomForestRegressionModel.load(MODEL_PATH)
        predictions = model.transform(df_final)
        
        print("--- PREDICTIONS ---")
        predictions.select("time", "features", "prediction").show(truncate=False)
    except Exception as e:
        print(f"❌ Error loading model: {e}")

if __name__ == "__main__":
    verify_model()
