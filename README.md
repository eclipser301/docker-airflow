
## Installation

Copy or clone the repository. 

Make sure the csv clickout files are in the ./data folder.
## Usage

Run the **CeleryExecutor**:

    docker-compose up -d
    
Force in the UI airflow view to trigger the dag or use command line below inside the webserver container:
    
     docker exec  -it container_id  /bin/bash
     airflow trigger_dag joblift_cpc_ETL
    
## DATA

all data are mounted in the following folders : 

-  ./data  has all the clickout csv files
-  ./results has the csv files stored (two staging files that contain the loading data and one final csv file and contains the last aggregated data)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)

##DEPLOYMENT:

For a production:
 - Better logging
 - Better management for environment variables (setting up as module and mount it as volume)
 - To deploy we need to build docker image again
 
 