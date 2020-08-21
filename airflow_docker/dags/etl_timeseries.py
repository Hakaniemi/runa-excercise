from datetime import datetime
 
import pandas as pd
import time

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


 ## Time series ELT Dag that will retrieve data from designed URL in json format and save it locally as cvs file
timestr = time.strftime("%Y%m%d-%H_%M")

dag = DAG(
   dag_id="etl_timeseries",
   start_date=datetime(2020, 8, 21),
   schedule_interval='@hourly', 
)
 
#fetch time series json data from URL 
extract= BashOperator(
   task_id="extract",
   bash_command='curl -o /opt/airflow/files/timeseries_{timestamp}.json https://pomber.github.io/covid19/timeseries.json'.format(timestamp=timestr),
   dag=dag,
)
 
 ## parse json data and writes it to output file
def parse_json(input_path, output_path):
   print("read_json ...")
   timeseries = pd.read_json(input_path)
   csv = timeseries
   csv.to_csv(output_path, index=False)
 
## task definition 
## 
transform_load = PythonOperator(
   task_id="transform_load",
   python_callable=parse_json,
   op_kwargs={
       "input_path": "/opt/airflow/files/timeseries_{timestamp}.json".format(timestamp=timestr),
       "output_path": "/opt/airflow/files/timeseries_{timestamp}.csv".format(timestamp=timestr),
   },
   dag=dag,
)
 
extract >> transform_load
