from datetime import datetime, timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 1. CONFIGURATION
# ============================================================

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    )
}


# ============================================================
# 2. EXTRACT + TRANSFORM
# ============================================================

def get_books_data(num_books, **context):

    ti = context["ti"]

    books = []
    seen_titles = set()

    page = 1
    max_pages = 10

    # Site de scraping
    base_url = "https://books.toscrape.com/catalogue/page-{}.html"

    # Conversion du rating
    rating_map = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5
    }

    print("=" * 60)
    print("STARTING BOOK SCRAPING")
    print("=" * 60)

    # ========================================================
    # SCRAPING DES PAGES
    # ========================================================

    while len(books) < num_books and page <= max_pages:

        url = base_url.format(page)

        print(f"\nScraping page {page}")
        print(f"URL: {url}")

        # ----------------------------------------------------
        # REQUEST
        # ----------------------------------------------------

        try:

            response = requests.get(
                url,
                headers=headers,
                timeout=10
            )

            response.raise_for_status()

        except requests.RequestException as e:

            print(
                f"ERROR while requesting page {page}: {e}"
            )

            break

        print(
            f"HTTP status: {response.status_code}"
        )

        # ----------------------------------------------------
        # BEAUTIFULSOUP
        # ----------------------------------------------------

        soup = BeautifulSoup(
            response.text,
            "html.parser"
        )

        # Chaque livre est dans :
        # <article class="product_pod">

        book_containers = soup.select(
            "article.product_pod"
        )

        print(
            f"Books found on page {page}: "
            f"{len(book_containers)}"
        )

        # Si aucun livre n'est trouvé,
        # on arrête pour éviter une boucle infinie.

        if not book_containers:

            print(
                "No books found. "
                "Stopping scraping."
            )

            break

        # ====================================================
        # EXTRACTION DES LIVRES
        # ====================================================

        for book in book_containers:

            # ------------------------------------------------
            # TITLE
            # ------------------------------------------------

            title_element = book.select_one(
                "h3 a"
            )

            # ------------------------------------------------
            # PRICE
            # ------------------------------------------------

            price_element = book.select_one(
                "p.price_color"
            )

            # ------------------------------------------------
            # RATING
            # ------------------------------------------------

            rating_element = book.select_one(
                "p.star-rating"
            )

            # ------------------------------------------------
            # AVAILABILITY
            # ------------------------------------------------

            availability_element = book.select_one(
                "p.instock.availability"
            )

            # Si aucun titre, on ignore le livre.

            if not title_element:
                continue

            # ------------------------------------------------
            # TITLE
            # ------------------------------------------------

            title = title_element.get("title")

            if not title:

                title = title_element.get_text(
                    strip=True
                )

            # ------------------------------------------------
            # PRICE
            # ------------------------------------------------

            if price_element:

                price = price_element.get_text(
                    strip=True
                )

            else:

                price = None

            # ------------------------------------------------
            # RATING
            # ------------------------------------------------

            rating = None

            if rating_element:

                classes = rating_element.get(
                    "class",
                    []
                )

                # Exemple :
                # ['star-rating', 'Three']

                if len(classes) > 1:

                    rating_word = classes[1]

                    rating = rating_map.get(
                        rating_word
                    )

            # ------------------------------------------------
            # AVAILABILITY
            # ------------------------------------------------

            if availability_element:

                availability = (
                    availability_element
                    .get_text(
                        strip=True
                    )
                )

            else:

                availability = None

            # ------------------------------------------------
            # REMOVE DUPLICATES
            # ------------------------------------------------

            if title not in seen_titles:

                seen_titles.add(title)

                books.append(
                    {
                        "Title": title,
                        "Price": price,
                        "Rating": rating,
                        "Availability": availability
                    }
                )

            # ------------------------------------------------
            # STOP WHEN WE HAVE ENOUGH BOOKS
            # ------------------------------------------------

            if len(books) >= num_books:

                break

        print(
            f"Total books collected: "
            f"{len(books)}"
        )

        page += 1

    # ========================================================
    # LIMIT TO num_books
    # ========================================================

    books = books[:num_books]

    # ========================================================
    # PANDAS
    # ========================================================

    df = pd.DataFrame(books)

    # Supprimer les doublons
    if not df.empty:

        df.drop_duplicates(
            subset="Title",
            inplace=True
        )

    print("\n" + "=" * 60)
    print("SCRAPING FINISHED")
    print("=" * 60)

    print(
        f"Final number of books: {len(df)}"
    )

    # Afficher les premières lignes dans les logs

    if not df.empty:

        print("\nSample data:")
        print(df.head())

    # ========================================================
    # XCOM
    # ========================================================

    ti.xcom_push(
        key="book_data",
        value=df.to_dict(
            "records"
        )
    )

    print(
        "\nData successfully pushed to XCom."
    )


# ============================================================
# 3. INSERT DATA INTO POSTGRESQL
# ============================================================

def insert_book_data_into_postgres(**context):

    # Récupération des données envoyées par le premier task

    book_data = context["ti"].xcom_pull(
        task_ids="fetch_book_data",
        key="book_data"
    )

    # --------------------------------------------------------
    # Vérification
    # --------------------------------------------------------

    if not book_data:

        raise ValueError(
            "No book data found in XCom."
        )

    print(
        f"Number of books received from XCom: "
        f"{len(book_data)}"
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    postgres_hook = PostgresHook(
        postgres_conn_id="books_connection"
    )

    # IMPORTANT :
    # Nouvelle table = books_scraped
    # L'ancienne table books n'est PAS touchée.

    insert_query = """
        INSERT INTO books_scraped (
            title,
            price,
            rating,
            availability
        )
        VALUES (%s, %s, %s, %s);
    """

    # --------------------------------------------------------
    # INSERT
    # --------------------------------------------------------

    for book in book_data:

        postgres_hook.run(
            insert_query,
            parameters=(
                book["Title"],
                book["Price"],
                book["Rating"],
                book["Availability"]
            )
        )

    print(
        f"{len(book_data)} books successfully "
        "inserted into books_scraped."
    )


# ============================================================
# 4. DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "start_date": datetime(
        2026,
        1,
        24
    ),

    "retries": 1,

    "retry_delay": timedelta(
        minutes=5
    )
}


# ============================================================
# 5. DAG
# ============================================================

dag = DAG(

    "books_etl_pipeline",

    default_args=default_args,

    description=(
        "ETL pipeline that extracts book data "
        "from Books to Scrape and stores it "
        "in PostgreSQL"
    ),

    schedule=timedelta(
        days=1
    ),

    catchup=False
)


# ============================================================
# 6. TASK 1 - EXTRACT + TRANSFORM
# ============================================================

fetch_book_data_task = PythonOperator(

    task_id="fetch_book_data",

    python_callable=get_books_data,

    op_kwargs={
        "num_books": 50
    },

    dag=dag
)


# ============================================================
# 7. TASK 2 - CREATE NEW TABLE
# ============================================================

create_table_task = SQLExecuteQueryOperator(

    task_id="create_table",

    conn_id="books_connection",

    sql="""
        CREATE TABLE IF NOT EXISTS books_scraped (

            id SERIAL PRIMARY KEY,

            title TEXT NOT NULL,

            price TEXT,

            rating INTEGER,

            availability TEXT

        );
    """,

    dag=dag
)


# ============================================================
# 8. TASK 3 - INSERT DATA
# ============================================================

insert_book_data_task = PythonOperator(

    task_id="insert_book_data",

    python_callable=insert_book_data_into_postgres,

    dag=dag
)


# ============================================================
# 9. TASK DEPENDENCIES
# ============================================================

fetch_book_data_task >> create_table_task >> insert_book_data_task