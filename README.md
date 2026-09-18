# Books ETL Pipeline with Apache Airflow

A personal Data Engineering project that extracts book data from [Books to Scrape](https://books.toscrape.com/), transforms it with Python, and loads it into PostgreSQL through an Apache Airflow workflow.

## Project Overview

This project demonstrates a complete ETL workflow:

```text
Books to Scrape
      |
      v
fetch_book_data
      |
      v
create_table
      |
      v
insert_book_data
      |
      v
PostgreSQL: books_scraped
```

The pipeline collects up to 50 books per run and stores their title, price, rating, and availability.

## Technologies

- Python
- Apache Airflow 3.1.6
- Docker and Docker Compose
- PostgreSQL
- Redis
- Requests
- BeautifulSoup
- Pandas
- SQL

## Airflow DAG

The DAG is defined in [`dags/dag.py`](dags/dag.py) and is named `books_etl_pipeline`.

It contains three dependent tasks:

1. `fetch_book_data`: requests up to three catalogue pages, extracts book information, removes duplicate titles, and sends the records to Airflow XCom.
2. `create_table`: creates the PostgreSQL table `public.books_scraped` if it does not already exist.
3. `insert_book_data`: reads the extracted records from XCom and inserts them into PostgreSQL.

The workflow is scheduled daily and configured with `catchup=False`.

## Run the Project

Open PowerShell in the project directory:

```powershell
cd "C:\Path\To\airflow_project"
Copy-Item .env.example .env
```

The `.env` file is intentionally ignored by Git. It contains local Docker settings; use `.env.example` as the public template.

Initialize the Airflow database and administrator account:

```powershell
docker compose up airflow-init
```

Start the services:

```powershell
docker compose up -d
```

Check the services:

```powershell
docker compose ps
```

Open the Airflow interface at [http://localhost:8080](http://localhost:8080).

Default local credentials:

```text
Username: airflow
Password: airflow
```

## Configure the PostgreSQL Connection

In Airflow, open **Admin > Connections** and create or update the connection with:

```text
Connection Id: books_connection
Connection Type: Postgres
Host: postgres
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

The host must be `postgres` because PostgreSQL is another service in the Docker Compose network.

## Run and Verify the DAG

In Airflow:

1. Find `books_etl_pipeline`.
2. Enable it if it is paused.
3. Click **Trigger DAG**.
4. Open the run and verify that all three tasks are green:
   - `fetch_book_data`
   - `create_table`
   - `insert_book_data`

Verify the result directly in PostgreSQL:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM public.books_scraped;"
```

Display sample records:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT title, price, rating, availability FROM public.books_scraped LIMIT 10;"
```

Expected result: the table contains the books collected by the successful DAG runs. Each manual run inserts up to 50 records.

## Evidence of a Successful Run

The repository can include screenshots showing:

1. Airflow Graph view with the three tasks completed successfully.
2. `fetch_book_data` logs showing HTTP status `200`, books found, and the final number of records.
3. PostgreSQL Query Tool showing `SELECT COUNT(*)` and sample rows from `books_scraped`.
4. Docker Compose services running with `docker compose ps`.

Add the images to `docs/screenshots/` using these names:

```text
docs/screenshots/01-airflow-dag-success.png
docs/screenshots/02-fetch-task-logs.png
docs/screenshots/03-postgres-results_1.png
docs/screenshots/03-postgres-results_2.png
docs/screenshots/04-docker-services.png
```

Then uncomment the following section and update the paths if necessary:

<!--
## Screenshots

### Airflow DAG succeeded

![Airflow DAG succeeded](docs/screenshots/01-airflow-dag-success.png)

### Extraction logs

![Extraction logs](docs/screenshots/02-fetch-task-logs.png)

### PostgreSQL results

![PostgreSQL results](docs/screenshots/03-postgres-results.png)

### Docker services

![Docker services](docs/screenshots/04-docker-services.png)
-->

## Project Structure

```text
.
├── dags/
│   └── dag.py
├── config/
│   └── airflow.cfg
├── plugins/
├── docs/
│   └── screenshots/
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Stop the Project

```powershell
docker compose down
```

The PostgreSQL data volume is preserved by default. To remove containers and stored database data, use the destructive command below only when that is intentional:

```powershell
docker compose down -v
```

## Limitations and Future Improvements

- Add a unique constraint or an upsert strategy to prevent duplicate rows across runs.
- Add automated tests for parsing and data validation.
- Store an extraction timestamp for each record.
- Move runtime dependencies into a custom Airflow image instead of installing them at container startup.
- Add monitoring and alerting for failed DAG runs.

## CV Summary

> Developed a Dockerized ETL pipeline with Apache Airflow to scrape book data, transform it with Python, and load it into PostgreSQL. Implemented dependent workflow tasks, scheduled execution, XCom data passing, and SQL-based result validation.

This is a personal project and a practical demonstration of ETL orchestration, Dockerized services, web data extraction, and relational data loading.
