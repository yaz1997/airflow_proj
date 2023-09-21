# Apache Airflow Project
This project aims to develop skills in using Apache Airflow, focusing on its different functionalities and configurations. 

## Prerequisites

* Begin with creating a virtual environment where you install Apache Airflow.
* Now, check for a folder in your /home/user directory.
* Verifying the installation using the "apache standalone" command in bash.
* Create a folder named dags inside the airflow folder where all the python files containing dags are stored.
* Create python files for necessary dags.

## Connections in Airflow UI 

#### HTTP Connection

* Define your connection ID.
* Connection Type: HTTP
* Host: Your API URL

![HTTP Connection Screenshot](/screenshots/http.png)

### Postgres Connection

* Define your Postgres Connection ID
* Connection Type: Postgres (If not found install Postgres dependencies)
* Host: localhost
* Schema: Your schema name
* Username: Your Postgres username 
* Password: Your Postgres Password
* Port: 5432

![HTTP Connection Screenshot](/screenshots/postgres.png)

### File sensor Connection 

* Define your file sensing connection ID
* Connection Type: File(path)
* Extra: Define your path

![HTTP Connection Screenshot](/screenshots/fs.png)


## Results:

### DAG dependencies:

![HTTP Connection Screenshot](/screenshots/dag.png)

### Final log output:

![HTTP Connection Screenshot](/screenshots/output.png)









