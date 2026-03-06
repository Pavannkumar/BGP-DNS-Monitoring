-- ══════════════════════════════════════════════════════════════════
-- BGP/DNS Route Monitoring Pipeline — Database Schema
-- ══════════════════════════════════════════════════════════════════

-- Table 1: Raw BGP route events
CREATE TABLE IF NOT EXISTS routing_events (
    id           SERIAL PRIMARY KEY,
    timestamp    TIMESTAMPTZ,
    router_id    VARCHAR(50),
    peer_as      INTEGER,
    origin_as    INTEGER,
    prefix       VARCHAR(50),
    event_type   VARCHAR(20),
    next_hop     VARCHAR(50),
    med          INTEGER,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Table 2: Raw DNS query events
CREATE TABLE IF NOT EXISTS dns_events (
    id             SERIAL PRIMARY KEY,
    timestamp      TIMESTAMPTZ,
    source_ip      VARCHAR(50),
    resolver_ip    VARCHAR(50),
    queried_domain VARCHAR(255),
    query_type     VARCHAR(10),
    response_code  VARCHAR(20),
    ttl            INTEGER,
    entropy        FLOAT,
    domain_length  INTEGER,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Table 3: Detected BGP anomaly alerts
CREATE TABLE IF NOT EXISTS bgp_alerts (
    id           SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    prefix       VARCHAR(50),
    origin_as    INTEGER,
    event_count  INTEGER,
    alert_type   VARCHAR(50),
    severity     VARCHAR(20),
    detected_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Table 4: Detected DNS anomaly alerts
CREATE TABLE IF NOT EXISTS dns_alerts (
    id           SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    source_ip    VARCHAR(50),
    event_count  INTEGER,
    alert_type   VARCHAR(50),
    severity     VARCHAR(20),
    detected_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Table 5: Aggregated metrics per window
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    metric_type     VARCHAR(50),
    metric_value    FLOAT,
    labels          VARCHAR(255),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);