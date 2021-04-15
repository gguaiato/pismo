# Pismo Challenge
The code in this repository has the purpose of show some 
 Apache Spark functionalities based on 
events provided by applications on a input directory.

## Events Definition ##
All events read by the job must be placed inside json 
files matching the schema below:
```
    {
 "event_id": "3aaafb1f-c83b-4e77-9d0a-8d88f9a9fa9a",
 "timestamp": "2021-01-14T10:18:57",
 "domain": "account",
 "event_type": "status-change",
 "data": {
     "id": 948874
     "random_fields": "random_data"
 }
}
``` 
> Note: The "data" field is parsed as a String by the job 
since it can contains different data from source to source.  

## Functionality ##
The written Spark Job can be found in the directory below:
> challenge/events_job.py

The job can be used to read json files in an input 
directory, deduplicate them based on an event_id using the 
latest timestamp and write them in a parquet format partitioned by domain, 
event_type, year, month and day.

Two parameters are needed when submiting a job for execution:
 * Input path with the json files containing the events emitted by applications
 * Output path where the parquet files should be written
 
## Requirements ##
The following requirements are needed in order to execute 
the Apache Spark Job:
* JDK 8.0
* Apache Spark 3.1.2
* Python 3.7

## Testing ##

Some automated tests assuring the properly functionality of the job 
were written with Pytest in the directory `challenge/tests`. They can be run 
using command:
```sh
 make test
```

### Requirements ###
Some requirements are needed to run the automated testes, they can be 
found `requirements.txt` file and installed with the following command:
```sh
pip install -r requirements.txt
``` 

### Simulation ###
Also, for testing purposes, a full simulation of the job execution 
can be triggered using a local HDFS system. was used and an  
integration makefile target triggering the full 
job execution is available with the command below:
```sh
 make run-local-with-sample
```
This execution will leave the output files 
in the path `hdfs://localhost/output-data`

## Deployment
The job deployment can be done by calling the 
spark-submit command with the two needed parameters:
```sh
spark-submit ./input_path ./output_path
```
 