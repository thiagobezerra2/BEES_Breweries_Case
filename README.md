# BEES_Breweries_Case


This project implements an ETL (Extract, Transform and Load) pipeline for brewery data using the Open Brewery DB API. The pipeline is orchestrated using Apache Airflow and is designed to run on the Astronomer platform.

## Project Structure

```plaintext
astro_airflow/
│
├── dags/
│   ├── extraction_api.py
│   ├── transform_data.py
│   ├── aggdata.py
│   └── brewery_data_pipeline.py
│
├── tests/
│   └── dags/
│       └── test_brewery.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── requirements.txt
└── Dockerfile
Astronomer configuration
Start a new Astronomer project:

Copy code
astro dev init
Copy all DAG scripts to the dags/ directory created by Astronomer.

Make sure that the Dockerfile is configured correctly:


Copy code
FROM quay.io/astronomer/astro-runtime:5.0.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
Start Astronomer:


Copy code
astro dev start
Running the Pipeline
In the Airflow UI (accessible via http://localhost:8080), activate the brewery_data_pipeline DAG.

Run the DAG manually to check that all the steps are working correctly.

Running Unit Tests
Run the tests with

Copy code
python -m unittest discover -s tests
DAG structure
extract_api.py: Extracts data from the Open Brewery DB API.
transform_data.py: Transforms the extracted data and partitions it by state.
aggdata.py: Aggregates the transformed data to create an analytical view.
brewery_data_pipeline.py: Orchestration of the ETL pipeline using Airflow.

Monitoring and Alerts
Use the Airflow UI to monitor the status of DAGs and tasks. Set up failure notifications as required.
