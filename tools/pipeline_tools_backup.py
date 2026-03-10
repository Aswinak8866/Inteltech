import os, subprocess, time, requests
from tools.airflow_tools import trigger_dag
import tools.data_tools as dt

OUTPUT_DIR   = "/home/vboxuser/de-ai-agent/outputs"
AIRFLOW_DAGS = "/home/vboxuser/airflow/dags"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def wait_for_dag(dag_id, timeout=60):
    url = f"http://localhost:8080/api/v1/dags/{dag_id}"
    for _ in range(timeout):
        try:
            r = requests.get(url, auth=("admin", "admin"))
            if r.status_code == 200:
                print(f"DAG '{dag_id}' is registered.")
                return True
        except Exception:
            pass
        time.sleep(1)
    return False

def run_full_pipeline(hdfs_path, table_name, dag_name):
    df       = dt.store["df"]
    filename = dt.store["filename"]
    if df is None:
        return "❌ No dataset loaded!", None

    steps = []
    hdfs_input  = f"{hdfs_path}/input/{table_name}.csv"
    hdfs_script = f"{hdfs_path}/scripts/{table_name}_iceberg.py"
    hdfs_output = f"{hdfs_path}/iceberg/{table_name}"

    # 1. Save CSV locally
    local_csv = f"{OUTPUT_DIR}/{table_name}.csv"
    df.to_csv(local_csv, index=False)
    steps.append(f"✅ 1. Saved locally → {local_csv}")

    # 2. Upload CSV to HDFS
    try:
        subprocess.run(["hdfs","dfs","-mkdir","-p",f"{hdfs_path}/input"], capture_output=True)
        r = subprocess.run(["hdfs","dfs","-put","-f",local_csv,hdfs_input],
                           capture_output=True, text=True, timeout=60)
        steps.append(f"✅ 2. Uploaded to HDFS → {hdfs_input}" if r.returncode==0
                     else f"⚠️ 2. HDFS upload warning: {r.stderr[:80]}")
    except Exception as e:
        steps.append(f"⚠️ 2. HDFS skipped: {str(e)[:60]}")

    # 3. Create PySpark Iceberg script
    cols = ", ".join([f'"{c}"' for c in df.columns])
    script = f"""from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("{table_name}_iceberg") \\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.local.type","hadoop") \\
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/data/iceberg-warehouse") \\
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("{hdfs_input}")
df.select({cols}).writeTo("local.default.{table_name}") \\
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: {hdfs_output}")
spark.stop()
"""
    script_path = f"{OUTPUT_DIR}/{table_name}_iceberg.py"
    with open(script_path,"w") as f:
        f.write(script)
    steps.append(f"✅ 3. PySpark Iceberg script created")

    # 4. Upload script to HDFS
    try:
        subprocess.run(["hdfs","dfs","-mkdir","-p",f"{hdfs_path}/scripts"], capture_output=True)
        r = subprocess.run(["hdfs","dfs","-put","-f",script_path,hdfs_script],
                           capture_output=True, text=True, timeout=60)
        steps.append(f"✅ 4. Script uploaded to HDFS → {hdfs_script}" if r.returncode==0
                     else f"⚠️ 4. Script upload: {r.stderr[:80]}")
    except Exception as e:
        steps.append(f"⚠️ 4. Skipped: {str(e)[:60]}")

    # 5. Create DAG file
    dag_code = f"""from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
with DAG(dag_id='{dag_name}', start_date=datetime(2024,1,1),
         schedule_interval=None, max_active_runs=1, catchup=False) as dag:
    run = BashOperator(
        task_id='run_{table_name}_iceberg',
        bash_command=(
            'spark-submit '
            '--conf spark.executor.memory=15g '
            '--conf spark.executor.instances=4 '
            '--conf spark.executor.cores=4 '
            '--jars /usr/local/lib/mssql-jdbc-12.2.0.jre8.jar '
            '{hdfs_script}'
        )
    )
"""
    dag_path = f"{OUTPUT_DIR}/{dag_name}.py"
    with open(dag_path,"w") as f:
        f.write(dag_code)

    # 6. Copy DAG to Airflow
    try:
        subprocess.run(["cp", dag_path, f"{AIRFLOW_DAGS}/{dag_name}.py"], check=True)
        steps.append(f"✅ 5. DAG '{dag_name}' copied to Airflow")
    except Exception as e:
        steps.append(f"⚠️ 5. DAG copy: {str(e)[:60]}")

    # 7. Poll until DAG is registered, then trigger
    registered = wait_for_dag(dag_name)
    if not registered:
        steps.append(f"⚠️ 6. DAG '{dag_name}' not found after 60s — trigger skipped")
    else:
        ok, msg = trigger_dag(dag_name)
        steps.append(f"{'✅' if ok else '⚠️'} 6. {msg}")

    steps.append(f"\n📦 Iceberg output: {hdfs_output}")

    return "\n".join(steps), dag_path

def generate_pyspark_code(table_name="my_table"):
    df = dt.store["df"]
    if df is None: return "❌ No data loaded.", None
    cols = ", ".join([f'"{c}"' for c in df.columns])
    code = f'from pyspark.sql import SparkSession\nspark = SparkSession.builder.appName("{table_name}").getOrCreate()\ndf = spark.read.parquet("hdfs:///data/{table_name}")\ndf.select({cols}).writeTo("spark_catalog.default.{table_name}").using("iceberg").createOrReplace()\n'
    path = f"{OUTPUT_DIR}/{table_name}_pyspark.py"
    with open(path,"w") as f: f.write(code)
    return f"✅ PySpark generated for `{table_name}`", path

def generate_dag(dag_name="my_pipeline", schedule="@daily"):
    code = f'from airflow import DAG\nfrom airflow.operators.bash import BashOperator\nfrom datetime import datetime\nwith DAG(dag_id="{dag_name}", start_date=datetime(2024,1,1), schedule_interval="{schedule}", catchup=False) as dag:\n    run = BashOperator(task_id="run", bash_command="echo done")\n'
    path = f"{OUTPUT_DIR}/{dag_name}_dag.py"
    with open(path,"w") as f: f.write(code)
    return f"✅ DAG generated: `{dag_name}`", path

def generate_sql(table_name="my_table"):
    df = dt.store["df"]
    if df is None: return "❌ No data loaded.", None
    sql = f"SELECT\n    {', '.join(df.columns)}\nFROM {table_name}\nLIMIT 100;"
    path = f"{OUTPUT_DIR}/{table_name}.sql"
    with open(path,"w") as f: f.write(sql)
    return f"✅ SQL generated for `{table_name}`", path

def detect_schema():
    df = dt.store["df"]
    if df is None: return "❌ No data loaded.", None
    schema = "\n".join([f"{c}: {df[c].dtype}" for c in df.columns])
    path = f"{OUTPUT_DIR}/schema.txt"
    with open(path,"w") as f: f.write(schema)
    return f"✅ Schema:\n```\n{schema}\n```", path

def build_pipeline(name="my_pipeline"):
    m1,f1 = generate_pyspark_code(name)
    m2,f2 = generate_dag(name)
    return f"✅ Pipeline built!\n{m1}\n{m2}", f1

def store_to_hdfs_iceberg(hdfs_path, table_name):
    """Store dataset directly to HDFS in Iceberg format — no Airflow."""
    df       = dt.store["df"]
    filename = dt.store["filename"]
    if df is None:
        return "❌ No dataset loaded!", None

    steps = []
    hdfs_input  = f"{hdfs_path}/input/{table_name}.csv"
    hdfs_script = f"{hdfs_path}/scripts/{table_name}_iceberg.py"
    hdfs_output = f"{hdfs_path}/iceberg/{table_name}"

    # 1. Save CSV locally
    local_csv = f"{OUTPUT_DIR}/{table_name}.csv"
    df.to_csv(local_csv, index=False)
    steps.append(f"✅ 1. Saved locally → {local_csv}")

    # 2. Upload CSV to HDFS
    try:
        subprocess.run(["hdfs","dfs","-mkdir","-p",f"{hdfs_path}/input"], capture_output=True)
        r = subprocess.run(["hdfs","dfs","-put","-f",local_csv,hdfs_input],
                           capture_output=True, text=True, timeout=60)
        steps.append(f"✅ 2. Uploaded to HDFS → {hdfs_input}" if r.returncode==0
                     else f"⚠️ 2. HDFS upload: {r.stderr[:80]}")
    except Exception as e:
        steps.append(f"⚠️ 2. HDFS skipped: {str(e)[:60]}")

    # 3. Create PySpark Iceberg script
    cols = ", ".join([f'"{c}"' for c in df.columns])
    script = f"""from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("{table_name}_iceberg") \\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.local.type","hadoop") \\
    .config("spark.sql.catalog.local.warehouse","hdfs://localhost:9000/data/iceberg-warehouse") \\
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("{hdfs_input}")
df.select({cols}).writeTo("local.default.{table_name}") \\
    .using("iceberg").tableProperty("write.format.default","parquet").createOrReplace()
print("✅ Written to Iceberg: {hdfs_output}")
spark.stop()
"""
    script_path = f"{OUTPUT_DIR}/{table_name}_iceberg.py"
    with open(script_path,"w") as f:
        f.write(script)
    steps.append(f"✅ 3. PySpark Iceberg script created")

    # 4. Upload script to HDFS
    try:
        subprocess.run(["hdfs","dfs","-mkdir","-p",f"{hdfs_path}/scripts"], capture_output=True)
        r = subprocess.run(["hdfs","dfs","-put","-f",script_path,hdfs_script],
                           capture_output=True, text=True, timeout=60)
        steps.append(f"✅ 4. Script uploaded → {hdfs_script}" if r.returncode==0
                     else f"⚠️ 4. {r.stderr[:80]}")
    except Exception as e:
        steps.append(f"⚠️ 4. Skipped: {str(e)[:60]}")

    # 5. Run PySpark script directly
    try:
        r = subprocess.run([
            "spark-submit",
            "--conf","spark.executor.memory=4g",
            script_path
        ], capture_output=True, text=True, timeout=300)
        if r.returncode == 0:
            steps.append(f"✅ 5. PySpark Iceberg job completed!")
            steps.append(f"📦 Data stored at: {hdfs_output}")
        else:
            steps.append(f"⚠️ 5. PySpark: {r.stderr[-200:]}")
    except Exception as e:
        steps.append(f"⚠️ 5. PySpark skipped: {str(e)[:80]}")

    return "\n".join(steps), script_path
