from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "jongmin",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 1, 4)
}

def print_current_date_with_jinja(date):
    execution_date = datetime.strptime(date, "%Y-%m-%d").date()
    date_kor = ["월", "화", "수", "목", "금", "토", "일"]
    datetime_weeknum = execution_date.weekday()
    print(f"{execution_date}는 {date_kor[datetime_weeknum]}요일입니다")

    return execution_date


with DAG(
    dag_id="python_dag_with_jinja",
    default_args=default_args,
    schedule_interval="30 0 * * *",
    tags=["my_dags"],
    catchup=True
) as dag:
    execution_date="{{ ds }}"

    python_task_jinja = PythonOperator(
        task_id="print_current_date_with_jinja",
        python_callable=print_current_date_with_jinja,
        # op_args=[execution_date]
        op_kwargs={
            "date": execution_date
        }
    )

    python_task_jinja