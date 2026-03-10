import os
def get_response(user_message, data_info="No data loaded."):
    msg = user_message.lower()

    # Airflow with schedule (must check before normal airflow)
    if any(k in msg for k in ["schedule","scheduled","cron"]):
        return "ACTION:RUN_AIRFLOW_SCHEDULE"

    # Normal airflow run
    if any(k in msg for k in ["airflow","run on","trigger"]):
        return "ACTION:RUN_AIRFLOW"

    if any(k in msg for k in ["push sql to hdfs","mysql to hdfs","sql to hdfs"]):
        return "ACTION:MYSQL_TO_HDFS"
    if any(k in msg for k in ["push to mysql","push to sql","store in mysql","save to mysql","store in sql"]):
        return "ACTION:PUSH_MYSQL"
    if any(k in msg for k in ["iceberg", "icebeg", "icberg", "isberg", "icebrg", "icebr", "icebe", "ice berg", "iceberg"]):
        return "ACTION:STORE_HDFS_ICEBERG"
    if any(k in msg for k in ["store","push","save","upload","hdfs"]):
        return "ACTION:STORE_HDFS"
    if any(k in msg for k in ["clean null","remove null","drop null"]):
        return "ACTION:REMOVE_NULLS:drop"
    if "fill null" in msg:
        return "ACTION:REMOVE_NULLS:fill"
    if "duplicate" in msg:
        return "ACTION:REMOVE_DUPLICATES"
    if "find null" in msg:
        return "ACTION:FIND_NULLS"
    if "schema" in msg:
        return "ACTION:DETECT_SCHEMA"
    if "pyspark" in msg:
        return "ACTION:GENERATE_PYSPARK:my_table"
    if "pipeline" in msg:
        return "ACTION:BUILD_PIPELINE:my_pipeline"
    if "sql" in msg and "mysql" not in msg:
        return "ACTION:GENERATE_SQL:my_table"
    if any(k in msg for k in ["mysql","load mysql","from mysql"]):
        return "ACTION:LOAD_MYSQL"
    return "❓ I did not understand. Try: push to hdfs, push to iceberg, push to mysql, run on airflow, cars run on airflow in schedule"
