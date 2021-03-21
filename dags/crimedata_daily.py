from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
 
 
result = PostgresHook(postgres_conn_id='postgres_new').get_uri().split("/")
dbURI = "/".join(result)

OAK_DATA_API = "https://data.oaklandca.gov/resource/ym6k-rx7a.json"

with DAG(dag_id="LOAD_DAILY_OAK_CRIMEDATA_PIPELINE", default_args=default_args, 
            catchup=False,template_searchpath="opt/airflow/", 
            description="get daily crime data onto psqldb") as dag:

    # @dag.task
    # def get_today_date() -> str:
    #     return datetime.now().strftime("%Y-%M-%D")
        

    @dag.task(default_args={"retries": "2", "retry_delay": timedelta(minutes=30)})
    def get_oak_crime_data_by_day():
        curr_date = datetime.now() - timedelta(days=1)
        yesterday = curr_date - timedelta(days=1)
        curr_date = curr_date.strftime("%Y-%m-%d")
        yesterday = yesterday.strftime("%Y-%m-%d")
        date_qparam = f"datetime between '{yesterday}T00:00:00' and '{curr_date}T00:00:00'"
        log.info(date_qparam)
        secret_key = Variable.get("SOCRATA_APPTOKEN")
        headers = {"X-App-Token": secret_key}
        s = requests.Session()
        s.headers.update(headers)
        try:
            #datetime between '2021-02-10T00:00:00' and '2021-02-10T23:59:59
            #$where=date between '2015-01-10T12:00:00' and '2015-01-10T14:00:00'
            #datetime=2021-02-02
            #"crimetype": "STOLEN VEHICLE"
            #"$where": "datetime between '2020-11-10T12:00:00' and '2021-01-10T14:00:00'"
            # "$where": "datetime between '2021-02-10T12:00:00' and '2021-02-11T14:00:00'"
            # "$where": "datetime > '2021-01-10'" 
            # params = { "$limit": "10", "$offset": "0", "$order": ":id", "$where": date_qparam}
            params = { "$order": ":id", "$where": date_qparam}
            resp = s.get(OAK_DATA_API, timeout=10, params=params)
            log.info("URL %s", resp.url)
            resp.raise_for_status()
        except HTTPError as http_err:
            log.error(http_err)
            # log.warning("Request failed with %s, request_id was %s with Err %s", resp.status_code, request_id, http_err)
        except Exception as err:
            # log.warning("Request failed with %s, request_id was %s", resp.status_code, request_id)
            log.error(err)
        else:
            log.info("HTTP Headers %s", resp.headers)
            # log.info("type %s",resp.headers["X-SODA2-Legacy-Types"])
            if not resp.json():
                raise ValueError("No Data fetched from API ", resp.content)
            log.info("succesful http resp" )
            data = resp.json()
            log.info("Retrieved %d crime data rows", len(data))
            log.info(data)
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