from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("abirami_iceberg") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/data") \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/data/input/abirami.csv")
df.select("date", "adj close", "close", "high", "low", "open", "volume", "ma_7", "ma_30", "ma_90", "daily_return", "volatility_7", "volatility_30", "rsi", "macd", "macd_signal", "bb_upper", "bb_lower").writeTo("local.default.abirami") \
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: hdfs://localhost:9000/data/default/abirami")
spark.stop()
