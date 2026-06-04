# Big-Data-Chicago-Air-Quality-Analysis

## Project Overview

This project applies Big Data technologies to analyze air quality sensor data from the city of **Chicago**, covering PM2.5 particulate matter, NO₂, temperature, humidity, and wind speed across 80+ neighborhoods.

The project is split into two major sections:

| Section | Approach | Technologies |
|---------|----------|--------------|
| **Section 1** | Batch processing | MapReduce · Spark SQL · HBase · Matplotlib |
| **Section 2** | Real-time stream processing | Spark Streaming · Cassandra · Grafana |

**Dataset:** [Chicago Air Quality Sensors — Chicago Data Portal](https://data.cityofchicago.org/)  
**Key metric:** PM2.5 concentration (µg/m³) — WHO threshold: 12 µg/m³

---

## Repository Structure

```
Big-Data-Chicago-Air-Quality-Analysis/
|
+-- chicago-bigdata/              # Section 1 - Batch Processing
|   +-- pom.xml
|   +-- src/main/java/tn/insat/bigdata/
|       +-- job1/                 # MapReduce - Data Cleaning
|       +-- job2/                 # MapReduce - Avg PM2.5 per sensor/day
|       +-- job3/                 # MapReduce - Top polluted neighborhoods
|       +-- job4/                 # MapReduce - Hourly PM2.5 distribution
|       +-- spark/                # Spark SQL - ChicagoAirAnalysis.java
|       +-- hbase/                # HBase import - ChicagoHBaseImport.java
|
+-- chicago-streaming/            # Section 2 - Stream Processing
|   +-- pom.xml
|   +-- src/main/java/tn/insat/bigdata/streaming/
|       +-- ChicagoStreamProcessor.java
|
+-- scripts/                      # Python utilities
|   +-- stream_chicago.py         # TCP socket stream emitter
|   +-- visualize.py              # Matplotlib visualizations
|
+-- .gitignore
+-- README.md
```

---

## Environment

| Component | Technology | Version |
|-----------|-----------|---------|
| Cluster | Docker (`liliasfaxi/hadoop-cluster`) | 1 master + 2 workers |
| Distributed storage | Apache Hadoop HDFS | 3.3.6 |
| Batch processing | Apache Spark | 3.5.0 |
| NoSQL (batch) | Apache HBase | 2.5.8 |
| Stream processing | Apache Spark Streaming | 3.5.0 (Scala 2.12) |
| NoSQL (streaming) | Apache Cassandra | 4.1.11 |
| Visualization | Grafana | latest |
| Build tool | Maven | Java 8 |
| OS | WSL2 Ubuntu 22.04 + Docker | — |

---

## Section 1 - Batch Processing

### Architecture

```
chicago_air.csv (HDFS)
    |
    v
Job 1 - Cleaning & filtering
    |
    v
Job 2 - Avg PM2.5 per sensor per day
Job 3 - Top polluted neighborhoods
Job 4 - Hourly PM2.5 distribution
    |
    v
Spark SQL - Advanced analyses (window functions, seasonality)
    |
    v
HBase - NoSQL storage (894,741 records)
    |
    v
Matplotlib - 4 visualizations
```

### MapReduce Jobs

| Job | Input | Output | Description |
|-----|-------|--------|-------------|
| Job 1 | Raw CSV (HDFS) | Cleaned TSV | Filters invalid rows, extracts PM2.5, neighborhood, coordinates |
| Job 2 | Job 1 output | Avg/Min/Max per sensor/day | Statistics per sensor per day |
| Job 3 | Job 1 output | Avg PM2.5 per neighborhood | Ranks most polluted neighborhoods |
| Job 4 | Job 1 output | Avg PM2.5 per hour | Identifies peak pollution hours |

### Spark SQL Analyses

- PM2.5 average, max, and count per neighborhood (1,304,069 valid rows processed)
- Hourly distribution across all 24h slots
- Monthly seasonality correlated with temperature
- Window function: most polluted sensor per day (rank)

### HBase Table

```
Table: chicago_air_quality
Row key: SENSOR_ID#YYYYMMDD#HH

Column families:
  measures → pm25, no2, temp, humidity
  meta     → neighborhood, lat, lon

Total records imported: 894,741
```

### Running Section 1

```bash
# Start the Docker cluster
docker start hadoop-master hadoop-worker1 hadoop-worker2
docker exec -it hadoop-master bash
./start-hadoop.sh

# Upload dataset to HDFS
hdfs dfs -mkdir -p /chicago/input
hdfs dfs -put /root/chicago_air.csv /chicago/input/

# Run MapReduce jobs in order
hadoop jar chicago-bigdata.jar tn.insat.bigdata.job1.PM25CleaningJob \
  /chicago/input /chicago/output/job1_cleaned

hadoop jar chicago-bigdata.jar tn.insat.bigdata.job2.AvgPM25Job \
  /chicago/output/job1_cleaned /chicago/output/job2_avg

hadoop jar chicago-bigdata.jar tn.insat.bigdata.job3.NeighborhoodPollutionJob \
  /chicago/output/job1_cleaned /chicago/output/job3_neighborhoods

hadoop jar chicago-bigdata.jar tn.insat.bigdata.job4.HourlyPollutionJob \
  /chicago/output/job1_cleaned /chicago/output/job4_hourly

# Run Spark analysis
spark-submit --class tn.insat.bigdata.spark.ChicagoAirAnalysis \
  --master yarn --deploy-mode client chicago-bigdata.jar

# Generate visualizations
python3 /root/visualize.py
```

### Key Results - Section 1

| Neighborhood | Avg PM2.5 (µg/m³) | Status vs WHO |
|---|---|---|
| Avalon Park | 13.83 | WARNING - Above threshold |
| Schiller Park Collocation | 12.34 | WARNING - Above threshold |
| North Lawndale | 11.97 | OK - Below threshold |
| New City | 11.83 | OK - Below threshold |

Peak pollution hours: **12h-14h** (avg ~11.83 µg/m³)  
Highest seasonal PM2.5: **December** (13.59 µg/m³) and **February** (13.68 µg/m³)

---

## Section 2 - Real-Time Stream Processing

### Architecture

```
stream_chicago.py          ChicagoStreamProcessor.java
(Python TCP emitter) --> (Spark Streaming) --> Cassandra --> Grafana
port 9999                 micro-batch 1 min    pm_realtime  dashboard
batch: 10 lines/3s        avg PM2.5/neighborhood UPSERT on key auto-refresh 1 min
```

### Pipeline Components

| Component | Role | Detail |
|-----------|------|--------|
| `stream_chicago.py` | Simulates IoT sensors | Reads CSV, emits 10 lines every 3s via TCP socket port 9999 |
| `ChicagoStreamProcessor.java` | Stream processing | Spark Streaming micro-batch 1 min - filters, maps, aggregates avg PM2.5 per neighborhood |
| Apache Cassandra | Real-time storage | UPSERT on `neighborhood` key - always stores latest value |
| Grafana | Live dashboard | Bar Gauge panel, color thresholds, auto-refresh every 1 min |

### Cassandra Schema

```sql
CREATE KEYSPACE chicago_air WITH replication = {
  'class': 'SimpleStrategy', 'replication_factor': 1
};

CREATE TABLE chicago_air.pm_realtime (
  neighborhood TEXT PRIMARY KEY,
  pm25         DOUBLE,
  last_update  TIMESTAMP
);
```

### Docker Network Setup

```bash
docker network create data-network
docker network connect data-network hadoop-master
docker network connect data-network cassandra-server
docker network connect data-network grafana-server
```

### Running Section 2 (3 terminals)

```bash
# Terminal 1 — Start Spark Streaming job
docker exec -it hadoop-master bash
spark-submit \
  --class tn.insat.bigdata.streaming.ChicagoStreamProcessor \
  --master local[2] chicago-streaming.jar

# Terminal 2 — Start the Python stream emitter
docker exec -it hadoop-master bash
python3 /root/stream_chicago.py

# Terminal 3 — Verify data in Cassandra
docker exec -it cassandra-server cqlsh
USE chicago_air;
SELECT * FROM pm_realtime;
```

### Grafana Dashboard

- URL: `http://localhost:3000` (admin/admin)
- Panel type: **Bar Gauge**
- Query: `SELECT neighborhood, pm25 FROM chicago_air.pm_realtime ALLOW FILTERING;`
- Color thresholds: GREEN < 12 µg/m³ | ORANGE 12-25 | RED > 25

### Key Results - Section 2

| Neighborhood | PM2.5 (µg/m³) | WHO Status |
|---|---|---|
| O'Hare | 20.0 | MODERATE |
| Englewood | 23.84 | MODERATE |
| Portage Park | 12.27 | MODERATE |
| Edison Park | 9.5 | GOOD |
| Avalon Park | 10.0 | GOOD |
| Beverly | 9.0 | GOOD |

80 neighborhoods tracked in real-time, updated every minute.

---

## Build Instructions

```bash
# Section 1 — chicago-bigdata
cd chicago-bigdata
mvn clean package
# Output: target/chicago-bigdata-1.0-jar-with-dependencies.jar

# Section 2 — chicago-streaming
cd chicago-streaming
mvn clean package
# Output: target/chicago-streaming-1-jar-with-dependencies.jar
```

---

## Visualizations

The `scripts/visualize.py` script generates 4 charts from MapReduce results:

1. **Top polluted neighborhoods** — horizontal bar chart with WHO threshold line
2. **Hourly PM2.5 distribution** — line chart with area fill
3. **Top 10 sensor measurements** — bar chart from Job 2
4. **Neighborhood pollution share** — pie chart (top 6 neighborhoods)
