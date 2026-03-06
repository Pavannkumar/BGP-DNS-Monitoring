# BGP/DNS Route Monitoring Pipeline

A real-time data engineering pipeline that monitors BGP routing events and DNS traffic to detect network anomalies such as route hijacks, DNS tunneling, and NXDOMAIN storms.


---

## Architecture
```
BGP Producer --> Kafka --> Spark Structured Streaming --> PostgreSQL --> Grafana
DNS Producer -->
```

Five microservices, fully containerised via Docker Compose:

| Service | Technology | Role |
|---|---|---|
| BGP Producer | Python | Simulates BGP routing events |
| DNS Producer | Python | Simulates DNS query events |
| Message Bus | Apache Kafka + Zookeeper | Event streaming backbone |
| Stream Processor | Apache Spark 3.5 | Anomaly detection + windowed aggregation |
| Database | PostgreSQL 16 | Persistent storage for events and alerts |
| Dashboard | Grafana 10.2 | Real-time visualisation |

---

## Anomaly Detection

- **BGP Route Flap** — HIGH alert when a prefix changes route more than 5 times in a 60-second window
- **BGP Hijack** — CRITICAL alert when an unknown AS originates a known prefix
- **DNS Tunneling** — CRITICAL alert when a source IP generates excessive DNS queries
- **NXDOMAIN Storm** — HIGH alert when NXDOMAIN response rate spikes

---

## Project Structure
```
bgp-dns-monitoring/
+-- docker-compose.yml        # Full infrastructure definition (IaC)
+-- producers/
¦   +-- bgp_producer.py       # BGP event simulator
¦   +-- dns_producer.py       # DNS event simulator
+-- spark/
¦   +-- bgp_processor.py      # BGP anomaly detection (Spark Structured Streaming)
¦   +-- dns_processor.py      # DNS anomaly detection (Spark Structured Streaming)
¦   +-- entrypoint.sh         # Launches both processors
¦   +-- Dockerfile
+-- postgres/
¦   +-- init.sql              # Schema: 5 tables auto-created on first run
+-- grafana/
    +-- dashboards/
        +-- monitoring.json   # Pre-built dashboard (auto-provisioned)
```

---

## Infrastructure as Code

The entire pipeline is defined in `docker-compose.yml`. No manual setup is required beyond Docker Desktop. All services, networks, volumes, and environment variables are declared and version-controlled.
