# Chicago Air Quality Streaming

A real-time data streaming and processing application for monitoring air quality across Chicago neighborhoods. This project combines Apache Spark Streaming for data ingestion and processing with Apache Cassandra for persistent storage and Grafana for real-time visualization with configurable alert thresholds.

## Project Overview

This project implements a complete real-time data pipeline for processing Chicago air quality measurements, specifically PM2.5 (fine particulate matter) concentrations. The system ingests streaming data through socket connections, processes batches of air quality readings by neighborhood, calculates averages, persists data to Cassandra, and provides real-time visualization capabilities.

## Architecture

### Components

**Java Backend (Spark Streaming)**
- Primary processing engine built on Apache Spark Streaming
- Receives streaming input via socket connection (TCP port 9999)
- Processes data in 1-minute batches
- Calculates average PM2.5 values per neighborhood
- Persists results to Apache Cassandra database

**Python Data Sources**
- `stream_chicago.py`: Data producer that reads from CSV source file and streams data to the Java backend via socket

**Visualization**
- Grafana: Real-time dashboards with configurable thresholds and alerts for air quality monitoring

**Data Flow**
1. CSV data source is read by stream_chicago.py
2. Data is sent via socket stream to Java backend (localhost:9999)
3. Spark Streaming aggregates and processes data in 1-minute windows
4. Results are stored in Cassandra database
5. Grafana queries Cassandra and displays real-time dashboards with alerts

## System Requirements

### Runtime Dependencies

**Java Environment**
- Java 8 (JDK 1.8 or higher)
- Apache Spark 3.5.0
- Apache Cassandra

**Python Environment**
- Python 3.x

**Visualization Requirements**
- Grafana server running (typically on port 3000)
- Grafana Cassandra datasource plugin or JDBC driver configured
- Network access between Grafana and Cassandra

**Data Requirements**
- Chicago air quality dataset from chicago portal in CSV format (chicago_air.csv)

### Build Dependencies

- Apache Maven 3.6.0+
- Maven compiler plugin 3.8.1+
- Maven assembly plugin 3.6.0+

## Technologies

- Apache Spark 3.5.0
  - spark-core_2.12
  - spark-streaming_2.12
  - spark-sql_2.12
- Apache Cassandra (via spark-cassandra-connector_2.12 3.5.0)
- Grafana for real-time visualization and alerting
- SLF4J with Reload4j for logging
- Python 3 for data production
- Maven for Java build automation

## Project Structure

```
chicago-streaming/
├── pom.xml                              # Maven project configuration
├── README.md                            
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── tn/insat/bigdata/
│   │   │       ├── Main.java           # Entry point
│   │   │       └── streaming/
│   │   │           └── ChicagoStreamProcessor.java  # Main Spark Streaming application
│   │   └── resources/                  # Configuration files
│   └── test/
│       └── java/                       # Test sources
├── stream_chicago.py                    # Data producer script
└── target/                              # Build output directory
    ├── classes/                        # Compiled class files
    ├── generated-sources/              # Generated source files
    └── maven-archiver/                 # Maven build metadata
```

## Installation and Setup

### Prerequisites

1. Install Java Development Kit (JDK) 8+
   ```bash
   # Verify Java installation
   java -version
   ```

2. Install Apache Spark 3.5.0 and set SPARK_HOME environment variable
   ```bash
   export SPARK_HOME=/path/to/spark-3.5.0
   export PATH=$SPARK_HOME/bin:$PATH
   ```

3. Install and start Apache Cassandra
   ```bash
   # Cassandra should be accessible at localhost:9042
   ```

4. Prepare your data file
   - Ensure chicago_air.csv is available at /root/chicago_air.csv
   - File must contain air quality measurements with neighborhood and PM2.5 data

5. Install and configure Grafana via Docker
   - Pull the latest Grafana image with Cassandra plugin support
   - Run the Docker container with port 3000 mapped
   - Connect Grafana container to the data-network for Cassandra access
   - Default login credentials are admin/admin
   - Configure Cassandra datasource with connection details

### Building the Project

Build the Java application using Maven:

```bash
cd chicago-streaming
mvn clean compile
mvn package
```

This generates:
- `chicago-streaming-1.jar`: Compiled application JAR
- `chicago-streaming-1-jar-with-dependencies.jar`: Executable JAR with all dependencies included

## Configuration

### Cassandra Configuration (ChicagoStreamProcessor.java)

The application connects to Cassandra with the following settings:
- Host: cassandra-server (modify `spark.cassandra.connection.host` for different hostname)
- Port: 9042 (default Cassandra port, modify `spark.cassandra.connection.port` if different)

Update these values in the SparkConf configuration within ChicagoStreamProcessor:

```java
SparkConf conf = new SparkConf()
    .setAppName("ChicagoAirQualityStreaming")
    .set("spark.cassandra.connection.host", "cassandra-server")
    .set("spark.cassandra.connection.port", "9042");
```

### Docker Network Setup

The Grafana and Cassandra containers communicate through a shared Docker network named `data-network`.

**Create the Docker network:**

```bash
docker network create data-network
```

**Start Cassandra container on the network:**

```bash
docker run --name cassandra-server --network data-network -p 9042:9042 -d cassandra:latest
```

**Connect Grafana to the network:**

```bash
docker network connect data-network grafana-server
```

This allows containers to communicate using DNS hostnames (e.g., `cassandra-server:9042`) instead of IP addresses.

### Grafana Datasource Configuration

**Docker Container Setup:**

Start Grafana with Cassandra plugin pre-installed:

```bash
docker run --name grafana-server -p 3000:3000 \
  -e "GF_INSTALL_PLUGINS=hadesarchitect-cassandra-datasource" \
  -d grafana/grafana:latest
```

**Cassandra Datasource Settings:**

In Grafana: Connections > Add new connection > Cassandra

| Field | Value | Notes |
|-------|-------|-------|
| Host | cassandra-server:9042 | Container hostname within data-network |
| Keyspace | chicago_air | Target Cassandra database |
| Consistency | ONE | Required - must not be left empty |
| Port | 9042 | Default Cassandra port |

Important: Leaving the Consistency field empty causes `gocql.ParseConsistencyWrapper: invalid consistency` errors. Always set it to a valid value (ONE, LOCAL_ONE, LOCAL_QUORUM, etc.).

**CQL Query for Dashboard Panel:**

```sql
SELECT neighborhood, pm25 FROM chicago_air.pm_realtime ALLOW FILTERING;
```

### Data Processing Configuration

**Streaming Parameters**
- Batch Duration: 1 minute (configurable via Durations.minutes(N))
- Socket Listen Port: 9999
- Host: localhost

**Data Producer Configuration (stream_chicago.py)**
- CSV File Path: /root/chicago_air.csv
- Server Host: localhost
- Server Port: 9999
- Batch Interval: 60 seconds
- Batch Size: 10 rows per transmission

**Grafana Configuration**
- Grafana Server: http://localhost:3000 (default)
- Docker Plugin: hadesarchitect-cassandra-datasource (pre-installed)
- Docker Network: data-network (connects to cassandra-server)
- Cassandra Host: cassandra-server:9042
- Cassandra Keyspace: chicago_air
- Cassandra Consistency: ONE (required, must not be left empty)
- Dashboard Refresh Interval: 1 minute (synchronized with Spark batches)
- Default Credentials: admin/admin
- Bar Gauge Panel Type with WHO air quality color thresholds:
  - Green: < 12 µg/m³
  - Orange: 12-25 µg/m³
  - Red: > 25 µg/m³

## Running the Application

### Terminal 1: Start the Java Spark Streaming Application

```bash
cd chicago-streaming
java -cp target/chicago-streaming-1-jar-with-dependencies.jar \
  tn.insat.bigdata.streaming.ChicagoStreamProcessor
```

The application will:
- Initialize the Spark Streaming context
- Listen on localhost:9999 for incoming data streams
- Print batch processing information to console

### Terminal 2: Start the Data Producer

```bash
python3 stream_chicago.py
```

The producer will:
- Read data from chicago_air.csv
- Connect to the Spark Streaming application
- Stream data in batches every 60 seconds
- Print connection and transmission status

### Terminal 3: Access Real-time Visualization via Grafana

The project uses Grafana for real-time visualization and monitoring with customizable thresholds and alerts. Grafana is deployed via Docker with the Cassandra datasource plugin pre-installed.

**Docker Setup**

Start the Grafana container with Cassandra plugin:

```bash
docker run --name grafana-server -p 3000:3000 \
  -e "GF_INSTALL_PLUGINS=hadesarchitect-cassandra-datasource" \
  -d grafana/grafana:latest
```

Connect Grafana to the Docker network (allows access to cassandra-server by hostname):

```bash
docker network connect data-network grafana-server
```

Default credentials:
- Username: admin
- Password: admin

**Cassandra Datasource Configuration**

In Grafana, navigate to: Connections > Add new connection > Cassandra

Configure the following fields:

| Field | Value | Description |
|-------|-------|-------------|
| Host | cassandra-server:9042 | Docker DNS hostname on data-network |
| Keyspace | chicago_air | Cassandra database name |
| Consistency | ONE | Consistency level (required - must be set) |

Important: The Consistency field must be set to a value. Leaving it empty causes gocql.ParseConsistencyWrapper errors.

**Dashboard and Panel Configuration**

Create a new dashboard with a Bar Gauge visualization panel.

CQL Query for the panel:

```sql
SELECT neighborhood, pm25 FROM chicago_air.pm_realtime ALLOW FILTERING;
```

Visualization Settings:

| Setting | Configuration | Purpose |
|---------|---------------|---------|
| Panel Type | Bar Gauge | Horizontal bar gauge display |
| Value Option | Last | Display only the latest value per neighborhood |
| Color Thresholds | Green (< 12), Orange (12-25), Red (> 25) | WHO air quality standards visualization |
| Threshold Units | micrograms/m³ (µg/m³) | PM2.5 measurement standard |
| Auto-refresh | 1 minute | Synchronized with Spark batch interval |

**Accessing the Dashboard**

Open your web browser and navigate to:

```
http://localhost:3000
```

The dashboard will display:
- Real-time PM2.5 measurements by neighborhood in Bar Gauge format
- Color-coded thresholds based on WHO air quality standards
- Historical trends with configurable time ranges
- Auto-refresh synchronized with 1-minute Spark streaming batches
- Neighborhood rankings by air quality levels
- Summary statistics and metrics

## Data Processing Details

### Data Format

Input data from CSV file:
- Row format: neighborhood,pm25_value
- Header row is automatically skipped
- Invalid or malformed entries are filtered out

### Processing Pipeline

1. **Data Ingestion**: Socket stream receives data batches
2. **Validation**: Lines containing null, empty, or header values are filtered
3. **Parsing**: Each line is split by comma to extract neighborhood and PM2.5 value
4. **Aggregation**: Data is grouped by neighborhood
5. **Calculation**: Sum and count of PM2.5 values per neighborhood are computed
6. **Averaging**: Average PM2.5 is calculated per neighborhood, rounded to 2 decimal places
7. **Persistence**: Results are written to Cassandra with timestamp
8. **Output**: Batch results are displayed in Spark console

### Cassandra Schema

Results are stored in Cassandra with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| neighborhood | String | Chicago neighborhood identifier |
| pm25 | Double | Average PM2.5 value (micrograms/m3) |
| last_update | Timestamp | Batch processing timestamp |

## Output and Monitoring

### Console Output

The application prints batch processing information:

```
========== BATCH [<timestamp>] ==========
<DataFrame display with 5 rows showing neighborhood, pm25, last_update>
```

### Logging

Logging is configured through SLF4J with Reload4j backend:
- Log level: Configurable through log4j.properties
- Output: Console and optional file appenders

### Grafana Dashboards

Grafana provides real-time visualization and monitoring:
- Neighborhood air quality trends with interactive charts
- PM2.5 concentration trends over time with adjustable time ranges
- Alert thresholds for unhealthy air quality conditions
- Custom dashboard layouts for different monitoring perspectives
- Historical data analysis and correlation detection

## Development

### Code Structure

**Main Classes**

- `ChicagoStreamProcessor.java`: Core streaming processor
  - Initializes SparkConf and JavaStreamingContext
  - Defines data transformation pipeline
  - Handles Cassandra writes

- `Main.java`: Entry point (currently minimal)

**Package Organization**
- `tn.insat.bigdata`: Core application package
- `tn.insat.bigdata.streaming`: Streaming-specific classes

### Building and Testing

```bash
# Clean previous builds
mvn clean

# Compile source code
mvn compile

# Run tests
mvn test

# Build complete package
mvn package

# Build with specific Java version
mvn clean package -DskipTests
```

### Modifying the Application

To extend the application:

1. **Add new data sources**: Create additional stream inputs in ChicagoStreamProcessor
2. **Modify aggregations**: Change grouping logic in mapToPair transformations
3. **Extend Cassandra schema**: Add columns to StructType schema definition
4. **Custom visualizations**: Create additional Grafana dashboards and panels with custom metrics

## Troubleshooting

### Connection Issues

**Problem: Cannot connect to Cassandra**
- Solution: Verify Cassandra is running and accessible at configured host:port
- Check firewall rules allow connections on port 9042

**Problem: Socket connection refused**
- Solution: Ensure Java application started before Python producer
- Verify port 9999 is not in use by another application

### Data Processing Issues

**Problem: No data appearing in Cassandra**
- Solution: Check Python data producer is running and connected
- Verify CSV file path is correct and accessible
- Check Cassandra connection settings in SparkConf

### Grafana and Datasource Issues

**Problem: Grafana cannot connect to Cassandra datasource**
- Solution: Ensure Grafana container is connected to data-network:
  ```bash
  docker network connect data-network grafana-server
  ```
- Verify Cassandra container is also on data-network:
  ```bash
  docker network inspect data-network
  ```

**Problem: gocql.ParseConsistencyWrapper: invalid consistency error**
- Solution: The Consistency field in Grafana datasource configuration must not be empty
- Set Consistency to a valid value: ONE, LOCAL_ONE, LOCAL_QUORUM, etc.
- Re-test the datasource connection after setting this field

**Problem: Grafana dashboard returns empty results or errors**
- Solution: Verify the CQL query syntax and table name:
  ```sql
  SELECT neighborhood, pm25 FROM chicago_air.pm_realtime ALLOW FILTERING;
  ```
- Check that the Keyspace and table exist in Cassandra
- Verify data is actually being written by Spark to Cassandra
- Check Grafana logs: `docker logs grafana-server`

**Problem: Grafana plugin installation fails**
- Solution: Ensure Docker image has plugin support enabled
- Use the full command with plugin environment variable:
  ```bash
  docker run -e "GF_INSTALL_PLUGINS=hadesarchitect-cassandra-datasource" ...
  ```

**Problem: Docker network connectivity issues**
- Solution: Create data-network first before starting containers:
  ```bash
  docker network create data-network
  docker run --network data-network --name cassandra-server ...
  docker run --network data-network --name grafana-server ...
  ```
- Verify connectivity between containers:
  ```bash
  docker exec grafana-server ping cassandra-server
  ```

**Problem: High numbers of INVALID records**
- Solution: Verify CSV format matches expected structure
- Check parse_line function in stream_chicago.py for correct column indices

### Memory Issues

**Problem: OutOfMemory errors during batch processing**
- Solution: Increase Spark executor memory:
  ```bash
  export SPARK_EXECUTOR_MEMORY=2g
  export SPARK_DRIVER_MEMORY=2g
  ```

## Performance Considerations

- Batch Duration: Default 1 minute balances latency and processing overhead
- Batch Size: 10 rows per transmission configured for network efficiency
- Cassandra Connection: Single connection from Spark cluster optimized for throughput
- Visualization Refresh: 60-second intervals balance real-time feedback with I/O load

## Security Considerations

- Socket stream operates on localhost only (not exposed to network)
- Cassandra connection should be secured in production environment
- CSV data file permissions should restrict unauthorized access
- Implement authentication for Cassandra in production deployments

## License

This project is part of an educational program at INSAT (Institut National des Sciences Appliquees et de Technologie).

## Support and Contact

For issues, questions, or contributions:
- Check troubleshooting section above
- Review Apache Spark Streaming documentation: https://spark.apache.org/docs/latest/streaming-programming-guide.html
- Review Apache Cassandra documentation: https://cassandra.apache.org/doc/latest/

## Future Enhancements

Potential improvements and extensions:

- Implement Kafka source for scalable data ingestion instead of socket streams
- Add data quality metrics and automated alerting with Grafana
- Extend Grafana visualization with geographic heat maps for neighborhood air quality
- Add Grafana alerting rules with webhook notifications
- Implement data retention policies in Cassandra for long-term storage management
- Add REST API for results access and external integrations
- Implement machine learning predictions for air quality forecasting using Spark MLlib
- Support multiple data sources and sensors with flexible routing
- Add comprehensive unit and integration tests
- Implement centralized configuration management system
- Add Docker Compose configuration for simplified multi-container orchestration
- Implement Elasticsearch integration for advanced log analysis
- Add Grafana annotations for event tracking and correlation analysis

## Version History

- Version 1.0: Initial release with Spark Streaming 3.5.0 and Cassandra integration
- Version 1.1: Added Docker-based Grafana deployment with Cassandra datasource for real-time dashboards and WHO air quality thresholds
