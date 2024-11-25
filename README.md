Here’s a draft of your `README.md` file for the project:

---

# **Real-Time Data Pipeline for Household Power Consumption**

## **Overview**
This project demonstrates an end-to-end data engineering pipeline that processes and analyzes real-time data streams using containerized tools. The dataset used is the [Household Power Consumption Dataset](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption).

## **Pipeline Architecture**
The project implements the following architecture:

1. **Source Data**:  
   - The input is a `.csv` file containing household power consumption data.
   - Data is read in chunks to simulate a real-time streaming source.

2. **Data Ingestion**:  
   - **Apache Airflow**: Orchestrates and schedules the entire data pipeline.  
   - A **DAG** loads `.csv` data and streams it into **Apache Kafka**.

3. **Message Streaming**:  
   - **Apache Kafka** (backed by **Zookeeper**): Handles real-time queuing of data.
   - Kafka topics are used to structure and manage the streaming data.

4. **Data Storage**:  
   - **Apache Cassandra**: Stores processed data for efficient querying and analysis.

5. **Data Processing**:  
   - **Apache Spark**: Consumes data from Kafka, processes it, and writes the output into Cassandra.

6. **Containerization**:  
   - All components (Airflow, Kafka, Zookeeper, Cassandra, Spark, etc.) are containerized using **Docker**.
   - **Docker Compose**: Simplifies the deployment of the multi-container architecture.

---

## **Setup Instructions**

### **Prerequisites**
1. **Docker** and **Docker Compose** installed.
2. **Python 3.11** with `pip` for local development.

### **Installation**
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Create a virtual environment and activate it:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start Docker containers:
   ```bash
   docker-compose up -d
   ```

---

## **Pipeline Components**

### **1. Apache Airflow**
- **Purpose**: Automates the execution of pipeline tasks.
- **DAG**:
  - Reads data from the `.csv` file in chunks.
  - Streams data into Kafka topics.
- Location of DAG: `dags/household_power_dag.py`.

### **2. Apache Kafka**
- **Purpose**: Manages real-time message queues for streaming data.
- **Topics**:
  - `power-data`: Houses raw data streamed from Airflow.

### **3. Apache Spark**
- **Purpose**: Processes Kafka messages and formats data.
- **Job**:
  - Transforms data (e.g., parsing timestamps, calculating summary metrics).
  - Writes processed data into Cassandra.

### **4. Apache Cassandra**
- **Purpose**: Serves as the database for processed data.
- **Schema**:
  ```sql
  CREATE TABLE power_consumption (
      id UUID PRIMARY KEY,
      timestamp TIMESTAMP,
      global_active_power FLOAT,
      voltage FLOAT,
      sub_metering_1 FLOAT,
      sub_metering_2 FLOAT,
      sub_metering_3 FLOAT
  );
  ```

---

## **Usage**

1. **Start the Pipeline**:
   Ensure all Docker containers are running:
   ```bash
   docker-compose up -d
   ```

2. **Load Data**:
   Place your `.csv` file (`household_power_consumption.csv`) in the `data/` directory.

3. **Trigger DAG**:
   Access the Airflow web UI at [http://localhost:8080](http://localhost:8080) and trigger the `household_power_dag`.

4. **Monitor Kafka**:
   - Use the **Kafka Control Center** at [http://localhost:9021](http://localhost:9021) to monitor Kafka topics and message flow.

5. **Query Processed Data**:
   - Access the Cassandra database:
     ```bash
     docker exec -it cassandra cqlsh
     ```
   - Run queries on the `power_consumption` table.

---

## **Project Directory Structure**
```plaintext
├── dags/
│   └── household_power_dag.py       # Airflow DAG for pipeline orchestration
├── data/
│   └── household_power_consumption.csv  # Input dataset
├── spark/
│   └── spark_streaming.py          # Spark streaming job for data processing
├── docker-compose.yml              # Docker Compose file
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```

---

## **Future Improvements**
- Add schema validation using **Schema Registry**.
- Enhance monitoring with tools like **Prometheus** and **Grafana**.
- Implement advanced analytics on processed data.

---

## **Acknowledgments**
- Dataset: [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption).
