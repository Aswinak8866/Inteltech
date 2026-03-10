from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
with DAG(dag_id='bmw_dag', start_date=datetime(2024,1,1),
         schedule_interval='52 2 * * *', max_active_runs=1, catchup=False) as dag:
    run = BashOperator(
        task_id='run_bmw_iceberg',
        bash_command=(
            'spark-submit '
            '--conf spark.executor.memory=15g '
            '--conf spark.executor.instances=4 '
            '--conf spark.executor.cores=4 '
            '--jars /usr/local/lib/mssql-jdbc-12.2.0.jre8.jar '
            'hdfs://localhost:9000/details/cars/scripts/bmw_iceberg.py'
        )
    )
