import json
import random
import time
import os
import sys
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Logging setup ─────────────────────────────────────────────────────────────
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [BGP-PRODUCER] %(message)s',
    stream=sys.stdout,
    force=True
)
log = logging.getLogger(__name__)

# ── Configuration from environment variables ──────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME              = os.getenv('TOPIC_NAME', 'bgp_raw_events')
EVENTS_PER_SECOND       = int(os.getenv('EVENTS_PER_SECOND', 100))

# ── Realistic BGP simulation data ─────────────────────────────────────────────
# Autonomous System numbers (real-world ISP/CDN AS numbers)
AS_NUMBERS = [
    15169,   # Google
    32934,   # Facebook
    16509,   # Amazon AWS
    8075,    # Microsoft
    13335,   # Cloudflare
    20940,   # Akamai
    1299,    # Telia
    3356,    # Lumen (CenturyLink)
    6461,    # Zayo
    2914,    # NTT
    7018,    # AT&T
    3320,    # Deutsche Telekom
    5400,    # BT
    12389,   # Rostelecom
    9002,    # RETN
]

# IP prefixes these ASes announce
IP_PREFIXES = [
    '8.8.0.0/24',     '8.8.4.0/24',
    '104.16.0.0/12',  '172.217.0.0/16',
    '52.0.0.0/11',    '13.32.0.0/15',
    '151.101.0.0/16', '23.0.0.0/12',
    '1.1.1.0/24',     '1.0.0.0/24',
    '192.30.252.0/22','185.199.108.0/22',
    '204.79.197.0/24','13.107.42.0/24',
    '198.41.128.0/17','162.158.0.0/15',
]

# Event types and their probabilities
# 70% normal, 20% withdraw, 10% path_change (anomaly injection)
EVENT_TYPES   = ['ANNOUNCE', 'WITHDRAW', 'PATH_CHANGE']
EVENT_WEIGHTS = [0.70,        0.20,       0.10]

def generate_as_path(origin_as):
    """Generate a realistic AS path from origin to a transit AS."""
    # Normally 2-4 hops
    num_hops = random.randint(2, 4)
    path = [origin_as]
    for _ in range(num_hops):
        path.append(random.choice(AS_NUMBERS))
    return path

def generate_bgp_event():
    """
    Generate one BGP route event with a realistic schema.
    
    Fields:
    - timestamp:   ISO 8601 UTC timestamp
    - router_id:   IP of the router that sent this update
    - peer_as:     The neighbouring AS that sent the update
    - origin_as:   The AS that originally announced the prefix
    - prefix:      The IP prefix being announced/withdrawn
    - event_type:  ANNOUNCE | WITHDRAW | PATH_CHANGE
    - as_path:     List of AS numbers from origin to peer
    - next_hop:    IP address of the next hop router
    - med:         Multi-Exit Discriminator (routing preference value)
    - communities: BGP community tags
    """
    origin_as  = random.choice(AS_NUMBERS)
    peer_as    = random.choice(AS_NUMBERS)
    prefix     = random.choice(IP_PREFIXES)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    as_path    = generate_as_path(origin_as)

    # Inject anomaly: route flap — same prefix announced then withdrawn rapidly
    # Spark will detect this with the 60s tumbling window
    if event_type == 'PATH_CHANGE':
        # Suspicious: AS path changed unexpectedly (potential hijack indicator)
        as_path.insert(1, random.randint(60000, 65000))  # unknown AS injected

    event = {
        'timestamp':   datetime.now(timezone.utc).isoformat(),
        'router_id':   f"10.{random.randint(0,255)}.{random.randint(0,255)}.1",
        'peer_as':     peer_as,
        'origin_as':   origin_as,
        'prefix':      prefix,
        'event_type':  event_type,
        'as_path':     as_path,
        'next_hop':    f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
        'med':         random.randint(0, 1000),
        'communities': [f"{random.randint(1,65535)}:{random.randint(1,65535)}"],
    }
    return event

def connect_producer():
    """Connect to Kafka with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # Serialise every event as JSON bytes
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Wait for all replicas to acknowledge (reliability)
                acks='all',
                # Retry up to 5 times on failure
                retries=5,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            log.warning(f"Kafka not ready yet, retrying in 5s... ({e})")
            time.sleep(5)

def main():
    producer      = connect_producer()
    event_count   = 0
    sleep_interval = 1.0 / EVENTS_PER_SECOND  # time between events

    log.info(f"Starting BGP event simulation → topic: {TOPIC_NAME}")
    log.info(f"Rate: {EVENTS_PER_SECOND} events/second")

    while True:
        try:
            event = generate_bgp_event()

            # Publish to Kafka
            # Key = prefix (ensures all events for same prefix go to same partition)
            producer.send(
                TOPIC_NAME,
                key=event['prefix'].encode('utf-8'),
                value=event
            )

            event_count += 1

            # Log progress every 1000 events
            if event_count % 1000 == 0:
                log.info(f"Published {event_count} BGP events")

            time.sleep(sleep_interval)

        except KeyboardInterrupt:
            log.info("Shutting down BGP producer...")
            break
        except Exception as e:
            log.error(f"Error publishing event: {e}")
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info(f"BGP producer stopped. Total events published: {event_count}")

if __name__ == '__main__':
    main()