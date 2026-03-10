from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ef_iceberg") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/ab/cd") \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/ab/cd/input/ef.csv")
df.select("order_id", "order_date", "ship_date", "delivery_date", "order_status", "customer_id", "customer_name", "country", "state", "city", "product_id", "product_name", "category", "sub_category", "brand", "quantity", "unit_price", "discount", "shipping_cost", "total_sales", "payment_method").writeTo("local.default.ef") \
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: hdfs://localhost:9000/ab/cd/default/ef")
spark.stop()
