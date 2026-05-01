# Data Engineering Project 2025-2026
**Enis Chavush & Oleksandr Uvarov — UC Leuven-Limburg**

---


## Yellow Taxi Dataset

The parquet file is not included in this repo because it's way too large for GitHub (~150MB).

Download it here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page


Once downloaded, place it here:
```
de_project/yellow_tripdata_2025-01.parquet
```

---

## Project Structure

```
de_project/
├── dags/
│   ├── batch_pipeline_dag.py         # DAG (Batch processing; Taxi dataset)
│   └── realtime_pipeline_dag.py      # DAG (Realtime processing; Sales dataset)
├── pipeline/
│   ├── reader.py                     
│   ├── validator.py                  
│   ├── processor.py                  
│   ├── backup_validator.py           
│   ├── writer.py                     
│   ├── realtime_validator.py         
│   └── realtime_processor.py        
├── validation_rules/
│   ├── batch_rules.json              # Taxi dataset validation rules
│   └── realtime_rules.json           # Sales dataset validation rules
├── input/                            # drop files here for sales dataset
│   └── processed/                    # files get moved here after processing
├── output/                           # processed files end up here
├── main.py                           # runs Batch processing manually
├── main_realtime.py                  # runs Realtime processing manually
├── generate_sample_data.py           # generates test data for sales dataset
└── requirements.txt
```

---

## Part 1 — Batch Processing

We read the Yellow Taxi parquet file, validate it, process it, and write the results locally and to Azure Blob Storage.

### What the processor does
- Removes: `VendorID`, `store_and_fwd_flag`, `RatecodeID`
- Adds these columns:

| Column | Description |
|--------|-------------|
| `trip_duration_minutes` | dropoff minus pickup in minutes |
| `average_speed_mph` | distance divided by duration in hours |
| `pickup_year` | year from pickup timestamp |
| `pickup_month` | month from pickup timestamp |
| `revenue_per_mile` | total amount divided by distance |
| `trip_distance_category` | Short / Medium / Long |
| `fare_category` | Low / Medium / High |
| `trip_time_of_day` | Night / Morning / Afternoon / Evening |

### Results
- ~2.85 million valid rows written to output and Azure
- ~626k rows invalidated for one reason or another

---

## Part 2 — Real-Time Processing

We built a pipeline that watches the `input/` folder. When a CSV or XLSX file gets dropped in there, it automatically runs through the pipeline and the results get written locally and to Azure.

### Dataset
We created a sales transactions dataset with 15 columns and 200 rows. You can generate it by running:
```bash
python generate_sample_data.py
```
This creates a clean version and a dirty version (with intentional errors) in the `input/` folder.

### What the processor adds
| Column | Description |
|--------|-------------|
| `revenue_after_discount` | total price after applying discount |
| `price_tier` | Budget / Mid-range / Premium |
| `transaction_hour` | hour of the transaction (0-23) |
| `is_high_value` | True if total price > 200 |
| `days_since_epoch` | days since 2025-01-01 |

It also removes duplicate rows based on `transaction_id`.

---

## Validation Rules

All validation rules are stored as JSON files in `validation_rules/` so they can be adjusted without touching the code.

### Part 1 (batch_rules.json)
- Mandatory columns must be non-null
- Dropoff must be after pickup
- Passenger count between 1 and 8
- Location IDs between 1 and 265
- Payment type must be 1-6
- Amounts must be >= 0

### Part 2 (realtime_rules.json)
- transaction_id must be unique
- Category must be one of the allowed values
- Quantity between 1 and 1000
- Unit price must be positive
- Discount percentage between 0 and 100

---

## Azure Blob Storage

Both pipelines upload their output to our Azure Blob Storage account (`enisoleksandr`, container: `yellow-taxi-processed`, region: France Central). The connection string is stored as an environment variable called `AZURE_STORAGE_CONNECTION_STRING`.

---

## Airflow

We used Airflow 2.10.0 running in WSL (Ubuntu) to schedule and orchestrate the pipelines.

- `yellow_taxi_batch_pipeline` - manually triggered on defence day
- `realtime_sales_pipeline` - runs every minute, monitors the input folder
