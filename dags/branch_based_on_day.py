from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def decide_what_to_run(**context):
    execution_date = context['execution_date']

    print("execution_date:", execution_date)
    print("weekday():", execution_date.weekday())
    print("day_of_week:", getattr(execution_date, 'day_of_week', 'N/A'))
    print("isoweekday():", execution_date.isoweekday())

    if execution_date.day_of_week > 4:  # 5 = sábado, 6 = domingo
        return 'special_task'
    else:
        return 'regular_task'

with DAG('branch_based_on_day',
         start_date=datetime(2025, 5, 1),
         schedule_interval='@daily',
         catchup=True) as dag:

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=decide_what_to_run,
        provide_context=True
    )

    special = EmptyOperator(task_id='special_task')
    regular = EmptyOperator(task_id='regular_task')
    join = EmptyOperator(task_id='join', trigger_rule='none_failed_min_one_success')

    branch >> [special, regular] >> join