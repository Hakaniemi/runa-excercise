# Veronica Castro 

Practical code excercise

### directory layout

    .
    ├── airflow docker                  		            # Airflow + postgress docker container configuration
        files                                                   # ETL generated Files
        Infrastructure and Administration.                # Pyhton Classes for the Infrastructure and Administration part
            ├── datalake.py                               # Allows constant connection to a data lake using sqlalchemy.
            ├── adding_c.py                               # Loads csv file form S3 (aws) addding a column to a data frame and creating a new csv file uploading to s3
            ├── test_datalake_connection.py               # Test Datalake class  		
            
 For docker container follow the next steps,  each one in a seperate terminal 
 
- docker-compose up postgres

- docker-compose up initdb

- docker-compose up scheduler webserver
