# Kafka Event Pipeline — Mock Order Events

A small distributed, event-driven pipeline: a **producer** generates mock
e-commerce order events and publishes them to a **Kafka** topic; a
**consumer** subscribes to that topic, processes each event, and persists
it to a local SQLite database. Everything runs in Docker containers,
orchestrated with Docker Compose, and is tested/verified automatically on
every push via GitHub Actions.

## Why this project

This demonstrates core distributed-systems concepts:
- **Decoupled communication** — producer and consumer don't know about
  each other directly; they only know about the Kafka topic.
- **Message queue / event-driven design** — events are published
  asynchronously and consumed independently, so the two services can run,
  restart, or scale on their own schedules.
- **Ordering** — each event is keyed by `order_id`, so events sharing a
  key are guaranteed to land on the same partition and be read in order.
- **Containers** — every component (Kafka, Zookeeper, producer,
  consumer) runs as an isolated container, defined declaratively in
  `docker-compose.yml`.

## Architecture

```
                 ┌──────────────┐        topic: order-events        ┌──────────────┐
                 │              │  ───────────────────────────────▶ │              │
                 │   Producer   │        (Kafka, partitioned,        │   Consumer   │
                 │  container   │         keyed by order_id)         │  container   │
                 │              │                                    │              │
                 └──────┬───────┘                                    └──────┬───────┘
                        │                                                   │
                        │        publishes JSON order events                │  writes each
                        ▼                                                   ▼  event to SQLite
                 ┌─────────────────────────────────────────┐        ┌──────────────┐
                 │      Kafka broker (+ Zookeeper)          │        │  orders.db   │
                 │   confluentinc/cp-kafka, single node     │        │  (SQLite)    │
                 └─────────────────────────────────────────┘        └──────────────┘
```

- `producer/producer.py` — generates a random mock order event
  (`order_id`, `user_id`, `item`, `quantity`, `price`, `timestamp`) every
  second and publishes it to the `order-events` Kafka topic.
- `consumer/consumer.py` — subscribes to `order-events`, and on each
  message, upserts a row into a local SQLite database (`orders.db`) and
  logs it to stdout.
- `docker-compose.yml` — spins up Zookeeper, a single-node Kafka broker,
  the producer container, and the consumer container, all networked
  together.
- `.github/workflows/ci.yml` — on every push/PR: installs dependencies,
  runs the test suite (`pytest`), and builds the Docker image. **A
  failing test fails the workflow**, which is enforced as a required
  check via branch protection (see below), blocking merges of broken
  code.

## Project structure

```
kafka-event-pipeline/
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── tests/
│   ├── test_producer.py
│   └── test_consumer.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .github/workflows/ci.yml
```

## How to run it

### Prerequisites
- Docker + Docker Compose installed

### Steps
```bash
# 1. Clone the repo
git clone https://github.com/<your-username>/kafka-event-pipeline.git
cd kafka-event-pipeline

# 2. Start everything (Zookeeper, Kafka, producer, consumer)
docker compose up --build

# 3. Watch the logs — you'll see the producer publishing events
#    and the consumer processing them, in real time.
```

To stop everything: `docker compose down`

### Running tests locally (without Kafka running)
```bash
pip install -r requirements.txt
pytest tests/ -v
```

## CI/CD and branch protection

Every push runs `.github/workflows/ci.yml`, which installs dependencies,
runs all tests, and builds the Docker image. To make this actually
**block** bad code from being merged:

1. Go to your GitHub repo → **Settings → Branches → Add branch
   protection rule**.
2. Branch name pattern: `main`.
3. Enable **"Require status checks to pass before merging"** and select
   the `test` check (from `ci.yml`).
4. Enable **"Require a pull request before merging"** so direct pushes
   to `main` go through a PR that must pass CI first.

With this enabled, a PR with a failing test cannot be merged into `main`.

## Possible extensions
- Add a second consumer group to show multiple independent subscribers.
- Add a REST endpoint (Flask/FastAPI) in front of the SQLite DB to query
  processed orders.
- Deploy the Kafka broker to a managed cloud service (e.g., Confluent
  Cloud or AWS MSK) instead of running it locally, for a "real" cloud
  deployment variant.
