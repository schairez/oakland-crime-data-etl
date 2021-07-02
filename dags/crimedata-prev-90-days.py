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
from crimedata.fetch_async import get_prev_90day_dataset
 


logging.basicConfig(format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


default_args = {
    "owner": "sergiochairez",
    "email": ['schairezv@gmail.com'],
    "email_on_failure": False,
    "dag_id": "LOAD_PREV90DAYS_OAK_CRIMEDATA_PIPELINE",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "start_date": days_ago(1),
    # "schedule_interval": "@daily",
    "catchup_by_default": False,
    "provide_context": True
}
 
 
result = PostgresHook(postgres_conn_id='postgres_new').get_uri().split("/")
dbURI = "/".join(result)

OAK_DATA_API = "https://data.oaklandca.gov/resource/ym6k-rx7a.json"

with DAG(default_args=default_args, 
            catchup=False,template_searchpath="opt/airflow/", 
            description="get prev 90dataset crime data onto psqldb") as dag:
 

    @dag.task(default_args={"retries": "2", "retry_delay": timedelta(minutes=30)})
    def get_oak_crime_data():
    
        secret_key = Variable.get("SOCRATA_APPTOKEN")
        data = get_prev_90day_dataset(secret_key)
        return data 
 
 
    @dag.task
    def load_data_to_db(data):
        log.info(dbURI)
        log.info("received data")
        log.info(data)
        dframe = json_normalize(data)
        engine = create_engine(dbURI)
        dframe.to_sql("crime_data", engine, index=False, schema="public", if_exists="append")

    
    get_oak_crime_data >> load_data_to_db 
    # load_data_to_db(get_oak_crime_data_by_day())

    #t0 >> get_oak_crime_data_by_day >> load_data_to_db 