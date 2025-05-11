from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import pytz


ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).strftime('%Y%m%d')

def task_A():
    n=0
    for i in range(10):
        n+=1    
        
with DAG('dag_1', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    doing_task_A = PythonOperator(
        task_id="file_exists",
        python_callable=task_A,
    )
    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_2',
        trigger_dag_id='dag_2',  
        conf={"date": str(today)} 
    )

    doing_task_A >> trigger_dag_b