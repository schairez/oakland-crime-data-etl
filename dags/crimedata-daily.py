from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

from sqlalchemy import create_engine
from datetime import datetime, timedelta

import requests 
from requests.exceptions import HTTPError
from requests.utils import requote_uri
from urllib.parse import urlencode, quote 

# from requests.adapters import HTTPAdapter

import os 
# from dotenv import load_dotenv
import logging
from pandas import json_normalize

# custom module
from crimedata.fetch import CrimeData


logging.basicConfig(format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


default_args = {
    "owner": "sergiochairez",
    "email": ['schairezv@gmail.com'],
    "email_on_failure": False,
    "dag_id": "LOAD_DAILY_OAK_CRIMEDATA",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "start_date": days_ago(1),
    "schedule_interval": "@daily",
    "catchup_by_default": False,
    "provide_context": True
}

#### pacific daylight time (UTC - 7)  spring 
#### pacific standard time (UTC - 8)  fall 
#### if we run @daily, from a 12am UTC perspective that's either 5pm in the spring or 4pm in the fall
#### so curr_date, yesterday would be 20th and 19th respectively if 12am UTC is on the 21st for ex
#### 

###  start = DummyOperator(task_id="start", dag=dag)
### start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
### end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

 
 
result = PostgresHook(postgres_conn_id='postgres_new').get_uri().split("/")
dbURI = "/".join(result)

OAK_DATA_API = "https://data.oaklandca.gov/resource/ym6k-rx7a.json"

with DAG(dag_id="LOAD_DAILY_OAK_CRIMEDATA_PIPELINE", default_args=default_args, 
            catchup=False,template_searchpath="opt/airflow/", 
            description="get daily crime data onto psqldb") as dag:

    # @dag.task
    # def get_today_date() -> str:
    #     return datetime.now().strftime("%Y-%M-%D")

    # start = DummyOperator(task_id='start', dag=dag)  
    #  end_operator = DummyOperator(task_id='stop', dag=dag)


    # @dag.task
    # def get_dates():



    @dag.task(default_args={"retries": "2", "retry_delay": timedelta(minutes=30)})
    def get_oak_crime_data_by_day():
        curr_date = datetime.now() - timedelta(days=1)
        yesterday = curr_date - timedelta(days=1)
        curr_date = curr_date.strftime("%Y-%m-%d")
        yesterday = yesterday.strftime("%Y-%m-%d")
        date_qparam = f"datetime between '{yesterday}T00:00:00' and '{curr_date}T00:00:00'"
        log.info(date_qparam)
        secret_key = Variable.get("SOCRATA_APPTOKEN")

        data = CrimeData.get_data(app_token=secret_key, date_qparam=date_qparam)
        return data 
 
 
    @dag.task
    def load_data_to_db(data):
        log.info(dbURI)
        log.info("received data")
        log.info(data)
        dframe = json_normalize(data)
        engine = create_engine(dbURI)
        dframe.to_sql("crime_data", engine, index=False, schema="public", if_exists="append")

    
    load_data_to_db(get_oak_crime_data_by_day())
    #t0 >> get_oak_crime_data_by_day >> load_data_to_db 