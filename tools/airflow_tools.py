import requests
from datetime import datetime

AIRFLOW_URL  = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"

def trigger_dag(dag_id):
    run_id = f"agent__{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        requests.patch(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
            json={"is_paused": False},
            auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10
        )
        r = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "conf": {}},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            headers={"Content-Type": "application/json"}, timeout=10
        )
        if r.status_code == 200:
            return True, f"✅ DAG '{dag_id}' triggered!\n🆔 Run ID: {run_id}\n🔗 Monitor: {AIRFLOW_URL}/dags/{dag_id}/grid"
        else:
            return False, f"❌ Failed ({r.status_code}): {r.text}"
    except Exception as e:
        return False, f"❌ Error: {str(e)}"
