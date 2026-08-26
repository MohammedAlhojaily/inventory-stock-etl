# Inventory & Stock ETL Pipeline

A daily Data Engineering pipeline for processing warehouse stock movements, calculating current inventory levels, and generating low-stock alerts.

## Architecture

```text
Inventory Master
      +
Daily Stock Movements
      ↓
     HDFS
      ↓
Hive Raw Table
      ↓
Clean Stock Movements
      ↓
Stock Balance
      ↓
Low Stock Alerts
      ↓
Airflow Orchestration
