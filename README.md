
# Maintenance Data Engineering Pipeline

## Overview
Production-style PySpark pipeline for maintenance analytics.

## Architecture
CSV → PySpark → Clean → Fact Table → Parquet/CSV → SQL

## How to Run
pip install -r requirements.txt
python run_pipeline.py
pytest tests/

