from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Fonction pour exécuter un script Python
def run_script(script_name):
    subprocess.run(["python3", script_name], check=True)

# Définition du DAG
with DAG(
    dag_id='staging_area',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 * * * *',  # Exécution quotidienne
    catchup=False,
) as dag:

    # Définition des tâches pour chaque script
    task1 = PythonOperator(
        task_id='run_script1',
        python_callable=run_script,
        op_args=['../notebook/script_name_basics_stock.py'],
    )

    task2 = PythonOperator(
        task_id='run_script2',
        python_callable=run_script,
        op_args=['../notebook/script_name_basics_stock.py'],
    )

    # Définir l'ordre des tâches
    task1 >> task2
