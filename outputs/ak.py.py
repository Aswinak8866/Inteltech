from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
with DAG(dag_id='ak.py', start_date=datetime(2024,1,1),
         schedule_interval=None, max_active_runs=1, catchup=False) as dag:
    run = BashOperator(
        task_id='run_asw.py_iceberg',
        bash_command=(
            'spark-submit '
            '--conf spark.executor.memory=15g '
            '--conf spark.executor.instances=4 '
            '--conf spark.executor.cores=4 '
            '--jars /usr/local/lib/mssql-jdbc-12.2.0.jre8.jar '
            'hdfs://localhost:9000/data/scripts/asw.py_iceberg.py'
        )
    )
