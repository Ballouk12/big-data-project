from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType

def main():
    spark = SparkSession.builder \
        .appName("Training_ML_Fixed_OHE") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Load Data (Preprocessed)
    # Ensure this path matches the output of PRETRAITEMENT_DATASET_ML.ipynb
    input_path = "hdfs://namenode-bd:9000/user/zeppelin/tfl_dataset_before_ml"
    print(f"📂 Loading data from {input_path}...")
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    print(f"✅ Loaded {df.count()} rows.")

    # 2. Ensure Numerical Types
    numeric_cols = ["data_temperature", "data_windspeed", "data_weathercode", "data_winddirection", "ts_hour", "is_weekend", "is_peak_hour"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    df = df.dropna(subset=numeric_cols)

    # 3. MANUAL ONE-HOT ENCODING (Standardizing with Inference)
    # Training notebook used StringIndexer (frequency based). 
    # Inference uses: afternoon (12-18), evening (>=18), morning (6-12). Night is reference.
    
    print("🛠️ Applying Manual One-Hot Encoding...")
    
    # We must ensure we rely on 'ts_hour' which should exist in the preprocessed data
    if "ts_hour" not in df.columns:
        print("❌ 'ts_hour' column missing!")
        return

    df = df \
        .withColumn("is_afternoon", when((col("ts_hour")>=12)&(col("ts_hour")<18), 1.0).otherwise(0.0)) \
        .withColumn("is_evening", when(col("ts_hour")>=18, 1.0).otherwise(0.0)) \
        .withColumn("is_morning", when((col("ts_hour")>=6)&(col("ts_hour")<12), 1.0).otherwise(0.0))

    # 4. Assemble Features
    # Exact order from realtime_inference.py
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
    df_prepared = assembler.transform(df)

    # 5. Split Data
    train_data, test_data = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Train size: {train_data.count()}, Test size: {test_data.count()}")

    # 6. Train Model
    print("🏋️ Training Random Forest Model...")
    rf = RandomForestRegressor(featuresCol="features", labelCol="data_temperature", numTrees=50)
    rf_model = rf.fit(train_data)
    
    # 7. Evaluate
    predictions = rf_model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="data_temperature", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"✅ RMSE: {rmse}")

    # 8. Save Model
    model_path = "hdfs://namenode-bd:9000/user/zeppelin/weather_rf_model_v2"
    print(f"💾 Saving model to {model_path}...")
    rf_model.write().overwrite().save(model_path)
    print("🎉 Done!")

if __name__ == "__main__":
    main()
