# Movie Recommendation System (MLOps)

## Overview
This project implements a production-grade movie recommendation system
using multiple models and Airflow orchestration.

## Data Sources
- PostgreSQL: source of truth (ratings, users, movies)
- Parquet Data Lake: training & evaluation data

## Models
| Model        | Purpose        | User Segment |
|-------------|----------------|--------------|
| Popularity  | Cold-start     | < 10 actions |
| ItemCF      | Warm users     | 10–30 actions |
| ALS (Spark) | Main backbone  | > 30 actions |

## Routing Strategy
- Cold users → Popularity
- Warm users → Item-based CF
- Hot users  → ALS

## Pipeline (Airflow DAGs)
1. ingest_ratings_to_parquet  
   - Export ratings from PostgreSQL to Parquet

2. train_als_model  
   - Train ALS model using Spark MLlib

3. generate_recommendations  
   - Generate batch recommendations
   - Write to PostgreSQL `recommendations` table

## Storage
- Data Lake: `data-lake/raw/ratings/`
- Model Artifacts: `data-lake/models/als/`
- Serving DB: PostgreSQL `recommendations`

## Evaluation
Offline evaluation using:
- Precision@K
- Recall@K
- Hit Rate@K

## Tech Stack
- Python 3.10
- PySpark
- PostgreSQL
- Airflow
- Pandas
- Parquet

## Notes
- Training and ingestion are fully decoupled
- Designed for scalability and reproducibility
- Suitable for MLOps coursework and production demos
