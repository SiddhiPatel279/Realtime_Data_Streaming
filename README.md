Real-Time Data Pipeline for Household Power Consumption
Overview
This project demonstrates an end-to-end data engineering pipeline that processes and analyzes real-time data streams using containerized tools. The dataset used is the Household Power Consumption Dataset.

Pipeline Architecture
The project implements the following architecture:

Source Data:

The input is a .csv file containing household power consumption data.
Data is read in chunks to simulate a real-time streaming source.
Data Ingestion:

Apache Airflow: Orchestrates and schedules the entire data pipeline.
A DAG loads .csv data and streams it into Apache Kafka.
Message Streaming:

Apache Kafka (backed by Zookeeper): Handles real-time queuing of data.
Kafka topics are used to structure and manage the streaming data.
Data Storage:

Apache Cassandra: Stores processed data for efficient querying and analysis.
Data Processing:

Apache Spark: Consumes data from Kafka, processes it, and writes the output into Cassandra.
Containerization:

All components (Airflow, Kafka, Zookeeper, Cassandra, Spark, etc.) are containerized using Docker.
Docker Compose: Simplifies the deployment of the multi-container architecture.
Setup Instructions
Prerequisites
Docker and Docker Compose installed.
Python 3.11 with pip for local development.
Installation
Clone the repository:

bash
Copy code
git clone <repository-url>
cd <repository-directory>
Create a virtual environment and activate it:

bash
Copy code
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
Install Python dependencies:

bash
Copy code
pip install -r requirements.txt
Start Docker containers:

bash
Copy code
docker-compose up -d
Pipeline Components
1. Apache Airflow
Purpose: Automates the execution of pipeline tasks.
DAG:
Reads data from the .csv file in chunks.
Streams data into Kafka topics.
Location of DAG: dags/household_power_dag.py.
2. Apache Kafka
Purpose: Manages real-time message queues for streaming data.
Topics:
power-data: Houses raw data streamed from Airflow.
3. Apache Spark
Purpose: Processes Kafka messages and formats data.
Job:
Transforms data (e.g., parsing timestamps, calculating summary metrics).
Writes processed data into Cassandra.
4. Apache Cassandra
Purpose: Serves as the database for processed data.
Schema:
sql
Copy code
CREATE TABLE power_consumption (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    global_active_power FLOAT,
    voltage FLOAT,
    sub_metering_1 FLOAT,
    sub_metering_2 FLOAT,
    sub_metering_3 FLOAT
);
Usage
Start the Pipeline: Ensure all Docker containers are running:

bash
Copy code
docker-compose up -d
Load Data: Place your .csv file (household_power_consumption.csv) in the data/ directory.

Trigger DAG: Access the Airflow web UI at http://localhost:8080 and trigger the household_power_dag.

Monitor Kafka:

Use the Kafka Control Center at http://localhost:9021 to monitor Kafka topics and message flow.
Query Processed Data:

Access the Cassandra database:
bash
Copy code
docker exec -it cassandra cqlsh
Run queries on the power_consumption table.
Project Directory Structure
plaintext
Copy code
├── dags/
│   └── household_power_dag.py       # Airflow DAG for pipeline orchestration
├── data/
│   └── household_power_consumption.csv  # Input dataset
├── spark/
│   └── spark_streaming.py          # Spark streaming job for data processing
├── docker-compose.yml              # Docker Compose file
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
Future Improvements
Add schema validation using Schema Registry.
Enhance monitoring with tools like Prometheus and Grafana.
Implement advanced analytics on processed data.
Acknowledgments
Dataset: UCI Machine Learning Repository.
