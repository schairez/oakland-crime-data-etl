# oakland-crime-data-etl

## Read about the api
https://dev.socrata.com/foundry/data.oaklandca.gov/ym6k-rx7a 


## start container init

docker-compose up airflow-init

## run 
docker-compose up 

## get container_id of airflow webserver
```docker ps```
get the container_id
then do

## set value of api_key
docker exec -it [CONTAINER_ID] airflow variables set SOCRATA_APPTOKEN [TOKEN_ID]

## get value of api_key to verify
docker exec -it [CONTAINER_ID] airflow variables get SOCRATA_APPTOKEN


<!-- ## set value of api_key
docker-compose run --rm webserver airflow variables --set SOCRATA_APPTOKEN value

 -->



## psql data addded
CONTAINER_ID for psql 

docker exec -it [CONTAINER_ID] bash
psql -U airflow
\dt to see list of relations 
airflow=# SELECT * FROM crime_data;



### check docker psql logs
docker logs [CONTAINER_ID]



### remove containers and volumes 
docker-compose down --volumes --rmi all





Data ingestion into psql
then 