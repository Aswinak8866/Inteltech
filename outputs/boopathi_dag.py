from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
with DAG(dag_id='boopathi_dag', start_date=datetime(2024,1,1),
         schedule_interval='2 10 * * *', max_active_runs=1, catchup=False) as dag:
    run = BashOperator(
        task_id='run_boopathi_iceberg',
        bash_command=(
            'spark-submit '
            '--conf spark.executor.memory=15g '
            '--conf spark.executor.instances=4 '
            '--conf spark.executor.cores=4 '
            '--jars /usr/local/lib/mssql-jdbc-12.2.0.jre8.jar '
            'hdfs://localhost:9000/aswin/ashwanth/scripts/boopathi_iceberg.py'
        )
    )
