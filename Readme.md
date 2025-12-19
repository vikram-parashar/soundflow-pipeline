![Metabase Dashboard](/dashboard.png)
# Soundflow

**Soundflow** is an end-to-end **data engineering project** that simulates music streaming events, ingests them into PostgreSQL, processes them through a **Bronze → Silver → Gold ETL architecture**, and exposes analytics-ready datasets for visualization in Metabase.

The project focuses on **data modeling, pipeline design, data quality, and analytics readiness**, rather than application development.

### Project Structure
``` 
soundflow/
├── docker-compose.yml        # Orchestrates Postgres, Metabase 
├── .env                      # Environment variables (not committed)
│
├── events/
│   └── Dockerfile            # Generates streaming-style events
│
├── scripts/
│   ├── generate_events.sh    # Event generation scripts
│   ├── run_etl.sh            # Runs ETL pipeline
│   └── ...                   # Other helper scripts
│
├── etl/
│   ├── bronze/               # Raw ingestion
│   ├── silver/               # Cleaned & standardized data
│   └── gold/                 # Analytics-ready models
│
├── sql/                      # table building sqls
│                             (see pg volume in docker-compose)
├── analytics/
│   ├── metrics/              # SQL definitions of business metrics -> For Metabase
│
└── README.md
```
### Event Generation
The  `events/` directory contains a Docker-based event generator that produces realistic music streaming data such as:

- authentication events
- song listen events
- page views
- subscription status changes

#### Credits
Event generation logic is based on **Interana Eventsim**:

> https://github.com/Interana/eventsim

The generator is adapted for local, containerized execution.
### Data Pipeline

#### Bronze Layer
- Raw ingestion from event streams
- Minimal validation
- Append-only
- Original payload preserved as JSON

#### Silver Layer
- Data cleaning and normalization
- Type casting and standardization
- Deduplication
- Domain validation (levels, auth states, timestamps)

#### Gold Layer
- Analytics-ready tables
- Business-level aggregations
- Stable grains and primary keys
- Optimized for BI tools

#### Example gold models:

- `daily_user_activity`
- `user_sessions`
- `daily_song_plays`
- `subscription_funnel_daily`
- `daily_geo_activity`
- `user_lifetime_metrics`
