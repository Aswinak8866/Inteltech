from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ai_worker_iceberg") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/data/iceberg-warehouse") \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/data/input/ai_worker.csv")
df.select("employee_id", "job_role", "years_experience", "education_level", "country", "industry", "company_size", "remote_work_type", "team_size", "salary_usd_k", "primary_ai_tool", "ai_tools_used_per_day", "hours_with_ai_assistance_daily", "ai_replaces_my_tasks_pct", "ai_adoption_stage", "weekly_ai_upskilling_hrs", "productivity_score", "burnout_score", "job_satisfaction_1_5", "fear_of_ai_replacement", "attrition_risk").writeTo("local.default.ai_worker") \
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: hdfs://localhost:9000/data/iceberg/ai_worker")
spark.stop()
