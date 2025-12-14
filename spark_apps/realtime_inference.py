from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, dayofweek, to_timestamp, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler

# --------------------------
# CONFIG
# --------------------------
KAFKA_BROKER = "kafka-bd:9092"
INPUT_TOPIC = "weather-live"
# Chemin HDFS du modèle entraîné
MODEL_PATH = "hdfs://namenode-bd:9000/user/zeppelin/weather_rf_model"

def main():
    spark = SparkSession.builder \
        .appName("WeatherRealTimeInference") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define Schema (matches data from Producer -> Flume -> Kafka)
    schema = StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("winddirection", DoubleType()),
        StructField("weathercode", DoubleType()),
        StructField("time", StringType()),
        StructField("ingest_time", DoubleType())
    ])

    # 2. Read Stream from Kafka
    print(f"⏳ Connecting to Kafka ({INPUT_TOPIC})...")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # 4. Feature Engineering (Must match training logic!)
    # a. Convert time string to timestamp
    df_features = df_parsed.withColumn("data_timestamp", to_timestamp(col("time")))
    
    # b. Rename columns to match training expected inputs
    df_features = df_features \
        .withColumnRenamed("windspeed", "data_windspeed") \
        .withColumnRenamed("weathercode", "data_weathercode") \
        .withColumnRenamed("winddirection", "data_winddirection")
        
    # c. Create Temporal Features
    df_features = df_features.withColumn("ts_hour", hour("data_timestamp"))
    df_features = df_features.withColumn("ts_day_of_week", dayofweek("data_timestamp"))
    
    # is_weekend (1=Sunday, 7=Saturday in Spark)
    df_features = df_features.withColumn("is_weekend",
                                         when((col("ts_day_of_week")==1)|(col("ts_day_of_week")==7), 1).otherwise(0))
    
    # is_peak_hour (7-10h et 16-19h)
    df_features = df_features.withColumn("is_peak_hour",
                                         when((col("ts_hour")>=7)&(col("ts_hour")<10), 1)
                                         .when((col("ts_hour")>=16)&(col("ts_hour")<19), 1).otherwise(0))
                                         
    # Ensure types match training
    df_features = df_features \
        .withColumn("is_weekend", col("is_weekend").cast(DoubleType())) \
        .withColumn("is_peak_hour", col("is_peak_hour").cast(DoubleType())) \
        .withColumn("ts_hour", col("ts_hour").cast(DoubleType()))

    # MANUAL ONE-HOT ENCODING for time_of_day
    # Logic matching PRETRAITEMENT_DATASET_ML.ipynb:
    # night: 0-6, morning: 6-12, afternoon: 12-18, evening: 18-24
    
    # We create the OHE columns directly: is_afternoon, is_evening, is_morning (alphabetical order usually used by StringIndexer/OHE)
    # Reference category is usually the last one alphabetically (night?), or first? 
    # Spark OneHotEncoder drops the last category by default.
    # Alphabetical: afternoon, evening, morning, night.
    # Dropped: night.
    # So we need: afternoon, evening, morning.
    
    df_features = df_features \
        .withColumn("is_afternoon", when((col("ts_hour")>=12)&(col("ts_hour")<18), 1.0).otherwise(0.0)) \
        .withColumn("is_evening", when(col("ts_hour")>=18, 1.0).otherwise(0.0)) \
        .withColumn("is_morning", when((col("ts_hour")>=6)&(col("ts_hour")<12), 1.0).otherwise(0.0))
        # night is when all are 0

    # ASSEMBLE FEATURES (Required for raw model)
    # Order must match training EXACTLY:
    # [data_windspeed, data_weathercode, data_winddirection, ts_hour, is_weekend, is_peak_hour, time_of_day_vec]
    # time_of_day_vec expands to: is_afternoon, is_evening, is_morning
    
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

    # 5. Load Model & Predict
    try:
        print(f"📂 Loading Model from {MODEL_PATH}...")
        # LOAD RAW MODEL instead of Pipeline
        model = RandomForestRegressionModel.load(MODEL_PATH)
        print("✅ Model loaded successfully!")
        
        predictions = model.transform(df_final)
        
        # Select output columns for display
        output_df = predictions.select(
            col("time"),
            col("temperature"),
            col("data_windspeed").alias("windspeed"),
            col("data_weathercode").alias("weathercode"),
            col("prediction").alias("predicted_temperature")
        )
        
    except Exception as e:
        print(f"❌ Critical Error loading model: {e}")
        print("⚠️ Falling back to displaying raw data...")
        output_df = df_parsed

    # 6. Write predictions to Console (not HBase)
    print("🚀 Starting Streaming - Predictions will be displayed below:")
    query = output_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
