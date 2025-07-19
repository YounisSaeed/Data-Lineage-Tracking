from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.db_utils import (
    get_table_schema,
    apply_schema_changes,
    table_exists,
    get_connection
)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
#########################################################################
def get_tables_to_monitor():
    """Return list of existing tables to monitor"""
    all_tables = ['customers', 'orders', 'products', 'customer_orders']
    return [t for t in all_tables if table_exists(t)]
#########################################################################
def check_table_changes(table_name, **context):
    from scripts.compare_snapshots import detect_changes
    
    changes = detect_changes(table_name)
    if changes:
        # Store changes in XCom for the next task
        context['ti'].xcom_push(key=f'changes_{table_name}', value=changes)
        return changes
    raise AirflowSkipException(f"No changes detected for {table_name}")
#########################################################################
def apply_table_changes(table_name, **context):
    ti = context['ti']
    changes = ti.xcom_pull(task_ids=f'check_changes_{table_name}', 
                         key=f'changes_{table_name}')
    
    if not changes:
        raise AirflowSkipException(f"No changes to apply for {table_name}")
    
    try:
        apply_schema_changes(table_name, changes)
        print(f"Successfully applied changes to {table_name}")
    except Exception as e:
        print(f"Failed to apply changes to {table_name}: {str(e)}")
        raise
#########################################################################
with DAG(
    'data_lineage_monitor',
    default_args=default_args,
    description='Monitor and track database schema changes',
    schedule_interval=timedelta(minutes=6),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['data_lineage'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    tables = get_tables_to_monitor()
    
    if not tables:
        no_tables = EmptyOperator(task_id='no_tables_found')
        start >> no_tables >> end
    else:
        for table in tables:
            take_snapshot_task = PythonOperator(
                task_id=f'take_snapshot_{table}',
                python_callable=get_table_schema,
                op_kwargs={'table_name': table},
                provide_context=True,
            )
            
            check_changes_task = PythonOperator(
                task_id=f'check_changes_{table}',
                python_callable=check_table_changes,
                op_kwargs={'table_name': table},
                provide_context=True,
            )
            
            apply_changes_task = PythonOperator(
                task_id=f'apply_changes_{table}',
                python_callable=apply_table_changes,
                op_kwargs={'table_name': table},
                provide_context=True,
            )
            
            start >> take_snapshot_task >> check_changes_task >> apply_changes_task >> end