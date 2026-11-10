# Masar Mini-Lakehouse

A small lakehouse built as the project of the course **SDA-DSC-214: Modern Data Engineering for AI Systems** at SDAIA Academy.

- Author: `<your name>`  
- Date: `<date>`  
- Official SDAIA Academy GitHub: [github.com/SDAIAAcademy](https://github.com/SDAIAAcademy)  
- Course starting repository: [altherwy/masar-lakehouse](https://github.com/altherwy/masar-lakehouse)

## What this project is

Masar is a made-up ride-hailing company in Saudi Arabia. The data is synthetic: trips, drivers and GPS pings for five cities. This project turns that raw data into trusted tables that serve both dashboards (BI) and machine learning (AI), following the lakehouse pattern: open file formats, a transaction log for reliability, and three layers of increasing quality.

## Architecture

```
 CSV / JSON files ----\
                       +--> BRONZE (raw, kept as it arrived, Delta)
 Kafka GPS stream ----/            |
                                   v
                     dbt model + checks --> SILVER (clean, enforced, Delta)
                                   |
                  quality contract --+--> GOLD (marts and features)
                                                |
                                +---------------+---------------+
                                v                               v
                       BI-safe table                     Training set
                  (pseudonymised, no raw ids)         (point-in-time features)
```

## What I built

| Day | Topic | What is in the repository |
| :---- | :---- | :---- |
| 1 | Bronze and silver | Landing scripts, profiling, dbt staging and silver models |
| 2 | Reliable tables and streaming | Delta constraints, schema evolution, MERGE, time travel, OPTIMIZE and VACUUM, Kafka to Delta streaming |
| 3 | Trust and serving | Data contract, quality gate, observability, PII retention, gold marts, one-command pipeline, training set, BI-safe table |

## Technology

- Python 3.11 and Java 17  
- Apache Spark 3.5.1 and Delta Lake 3.2.0  
- dbt 1.8 with the Spark adapter  
- Apache Kafka 3.7.0 (single broker, run with Docker)  
- Great Expectations 0.18.19  
- GitHub Codespaces as the development environment

## Project structure

```
src/masar/
  ingest/       landing raw files into bronze
  delta/        constraints, MERGE, time travel, small files
  stream/       Kafka producer and streaming ingest
  quality/      data contract, gate, observability, PII retention
  transform/    gold tables
  serve/        training set and BI-safe table
  pipeline.py   the one-command pipeline
dbt/masar/      dbt staging and silver models and tests
Makefile        shortcuts such as make data and make pipeline
```

## How to run it

The data is not stored in this repository. You generate it with the steps below.

1. Open the repository in a GitHub Codespace (Code button, then Codespaces, then Create). Wait until the setup finishes.  
2. Check the environment:

```sh
make doctor
```

3. Generate the dataset:

```sh
make data
```

4. Land the raw files into bronze (run this once only):

```sh
python -m masar.ingest.land_bronze
```

5. Register the bronze tables and install dbt packages:

```sh
make register
make dbt-deps
```

6. Build and test the silver table, then add the Delta constraints:

```sh
cd dbt/masar && dbt run && dbt test && cd ../..
python -m masar.delta.enforce_silver
```

7. Run the whole pipeline (silver, quality check, gold, maintenance):

```sh
make pipeline
```

8. Serve the two consumers:

```sh
python -m masar.serve.training_join
python -m masar.serve.bi_view
```

Only one Spark session can run at a time, so wait for each command to finish before starting the next.

## Results

- Silver: about 44,371 trips (44,364 if you also ran the Day 2 erase of one rider)  
- Data contract on silver: 7 of 7 rules pass  
- Gold demand table: 120 rows (5 cities x 24 hours); gold features: one row per trip  
- Pipeline: runs twice with identical counts, and recovers after an injected failure  
- BI table: no raw rider or driver ids; the same number of pseudonyms as riders

## Limits of this project

This is a teaching lakehouse on one machine with synthetic data. It is not production-ready: it has no real catalog or access control, no scheduler or alerting, and the streaming part is not connected to the main pipeline.

## Acknowledgements

Built during SDA-DSC-214 at [SDAIA Academy](https://github.com/SDAIAAcademy), starting from the course repository above.
