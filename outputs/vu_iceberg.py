from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("vu_iceberg") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/yz/wx") \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/yz/wx/input/vu.csv")
df.select("car_id", "model", "year", "engine_size", "horsepower", "fuel_type", "transmission", "drivetrain", "mileage_km", "fuel_consumption_l_per_100km", "co2_emissions_g_km", "price_usd", "doors", "seats", "body_type", "color", "owner_count", "accident_history", "service_history", "country_sold").writeTo("local.default.vu") \
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: hdfs://localhost:9000/yz/wx/default/vu")
spark.stop()
