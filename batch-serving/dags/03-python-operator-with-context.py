from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args={
    "owner": "jongmin",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 1, 4)
}

def print_current_date_with_context(*args, **kwargs):
    execution_date = kwargs["ds"]
    execution_date_nodash = kwargs["ds_nodash"]
    print(f"execution_date_nodash : {execution_date_nodash}")
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    date_kor = ["월", "화", "수", "목", "금", "토", "일"]
    datetime_weeknums = execution_date.weekday()
    print(f"{execution_date}는 {date_kor[datetime_weeknums]}요일입니다")

with DAG(
    dag_id="python_dag_with_context",
    default_args=default_args,
    schedule_interval="30 0 * * *",
    tags=["my_dags"],
    catchup=True
) as dag:
    
    PythonOperator(
        task_id="print_current_date_with_context",
        python_callable=print_current_date_with_context
    )
    