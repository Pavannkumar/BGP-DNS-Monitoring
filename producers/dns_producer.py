import json
import random
import time
import os
import sys
import logging
import hashlib
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Logging setup ─────────────────────────────────────────────────────────────
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [DNS-PRODUCER] %(message)s',
    stream=sys.stdout,
    force=True
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME              = os.getenv('TOPIC_NAME', 'dns_raw_events')
EVENTS_PER_SECOND       = int(os.getenv('EVENTS_PER_SECOND', 100))

# ── Realistic DNS simulation data ─────────────────────────────────────────────
# Normal domains
NORMAL_DOMAINS = [
    'google.com',       'youtube.com',    'facebook.com',
    'github.com',       'stackoverflow.com', 'amazon.com',
    'microsoft.com',    'apple.com',      'cloudflare.com',
    'netflix.com',      'twitter.com',    'linkedin.com',
    'wikipedia.org',    'reddit.com',     'instagram.com',
]

# Non-existent domains (trigger NXDOMAIN responses)
# Spark will detect a storm if > 100 NXDOMAIN per source IP per 60s
NXDOMAIN_DOMAINS = [
    'notexist-abc123.com',  'fake-domain-xyz.net',
    'random-nxdomain.org',  'does-not-exist-999.io',
    'invalid-host-test.com','nxdomain-example.net',
]

# DNS query types
QUERY_TYPES = ['A', 'AAAA', 'MX', 'TXT', 'CNAME', 'NS', 'PTR', 'ANY']

# Response codes
RESPONSE_CODES = {
    'NOERROR':  0,   # Successful query
    'NXDOMAIN': 3,   # Domain does not exist
    'SERVFAIL': 2,   # Server failure
    'REFUSED':  5,   # Query refused
}

def generate_tunneling_subdomain():
    """
    Generate a suspicious DNS tunneling subdomain.
    DNS tunneling encodes data in very long subdomains.
    Spark detects this via high entropy + length > 50 chars.
    Example: a3f8b2c1d4e5.exfiltrate-data.evil.com
    """
    # Random hex-like string mimicking encoded data
    encoded_data = ''.join(
        random.choices('abcdef0123456789', k=random.randint(40, 60))
    )
    base_domain = random.choice(['evil.com', 'malware.net', 'c2-server.org'])
    return f"{encoded_data}.{base_domain}"

def calculate_entropy(domain):
    """
    Shannon entropy — measures randomness of a string.
    Normal domains: low entropy (e.g. google.com = ~2.5)
    Tunneling domains: high entropy (e.g. a3f8b2c1...evil.com = ~3.8+)
    """
    if not domain:
        return 0
    freq = {}
    for c in domain:
        freq[c] = freq.get(c, 0) + 1
    import math
    entropy = 0
    for count in freq.values():
        p = count / len(domain)
        entropy -= p * math.log2(p)
    return round(entropy, 3)

def generate_source_ip():
    """Generate a realistic source IP address (hashed for PII protection)."""
    raw_ip = f"192.168.{random.randint(1,10)}.{random.randint(1,50)}"
    # SHA-256 hash — raw IP never stored (GDPR compliance)
    hashed = hashlib.sha256(raw_ip.encode()).hexdigest()[:16]
    return hashed

def generate_dns_event():
    """
    Generate one DNS query event.

    Fields:
    - timestamp:      ISO 8601 UTC timestamp
    - source_ip:      SHA-256 hashed source IP (PII protected)
    - resolver_ip:    DNS resolver that handled the query
    - queried_domain: The domain that was looked up
    - query_type:     A | AAAA | MX | TXT | CNAME etc.
    - response_code:  NOERROR | NXDOMAIN | SERVFAIL | REFUSED
    - response_time:  Query round-trip time in milliseconds
    - ttl:            Time-to-live of the DNS response
    - entropy:        Shannon entropy of the queried domain
    - is_suspicious:  Flag for downstream Spark detection
    """
    # Determine event category
    # 75% normal, 15% NXDOMAIN, 10% tunneling
    category = random.choices(
        ['normal', 'nxdomain', 'tunneling'],
        weights=[0.75, 0.15, 0.10]
    )[0]

    if category == 'normal':
        domain        = random.choice(NORMAL_DOMAINS)
        response_code = 'NOERROR'
        query_type    = random.choices(
            QUERY_TYPES,
            weights=[0.6, 0.1, 0.05, 0.1, 0.05, 0.05, 0.025, 0.025]
        )[0]
        is_suspicious = False

    elif category == 'nxdomain':
        domain        = random.choice(NXDOMAIN_DOMAINS)
        response_code = 'NXDOMAIN'
        query_type    = 'A'
        is_suspicious = True

    else:  # tunneling
        domain        = generate_tunneling_subdomain()
        response_code = random.choice(['NOERROR', 'NXDOMAIN'])
        query_type    = random.choice(['TXT', 'ANY', 'CNAME'])
        is_suspicious = True

    event = {
        'timestamp':      datetime.now(timezone.utc).isoformat(),
        'source_ip':      generate_source_ip(),
        'resolver_ip':    f"8.8.{random.randint(0,1)}.{random.randint(1,8)}",
        'queried_domain': domain,
        'query_type':     query_type,
        'response_code':  response_code,
        'response_time':  round(random.uniform(0.5, 500.0), 2),
        'ttl':            random.choice([60, 300, 3600, 86400]),
        'entropy':        calculate_entropy(domain),
        'is_suspicious':  is_suspicious,
    }
    return event

def connect_producer():
    """Connect to Kafka with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            log.warning(f"Kafka not ready yet, retrying in 5s... ({e})")
            time.sleep(5)

def main():
    producer       = connect_producer()
    event_count    = 0
    sleep_interval = 1.0 / EVENTS_PER_SECOND

    log.info(f"Starting DNS event simulation → topic: {TOPIC_NAME}")
    log.info(f"Rate: {EVENTS_PER_SECOND} events/second")

    while True:
        try:
            event = generate_dns_event()

            # Key = hashed source IP (routes same IP to same partition)
            producer.send(
                TOPIC_NAME,
                key=event['source_ip'].encode('utf-8'),
                value=event
            )

            event_count += 1

            if event_count % 1000 == 0:
                log.info(f"Published {event_count} DNS events")

            time.sleep(sleep_interval)

        except KeyboardInterrupt:
            log.info("Shutting down DNS producer...")
            break
        except Exception as e:
            log.error(f"Error publishing event: {e}")
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info(f"DNS producer stopped. Total events published: {event_count}")

if __name__ == '__main__':
    main()