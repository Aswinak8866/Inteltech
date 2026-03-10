from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("path_iceberg") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/wrong/fake") \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/wrong/fake/input/path.csv")
df.select("order_id", "order_date", "ship_date", "delivery_date", "order_status", "customer_id", "customer_name", "country", "state", "city", "product_id", "product_name", "category", "sub_category", "brand", "quantity", "unit_price", "discount", "shipping_cost", "total_sales", "payment_method").writeTo("local.default.path") \
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: hdfs://localhost:9000/wrong/fake/default/path")
spark.stop()
