# PySpark ETL Pipeline

This project is a simple PySpark ETL pipeline that reads raw CSV data, cleans and transforms it, and writes the output to Parquet and Delta formats.

## Project Structure

```
pyspark-pipeline/
|-- data/
|   |-- raw/
|   |-- processed/
|-- src/
|   |-- __init__.py
|   |-- main.py
|   |-- etl.py
|   |-- utils.py
|-- tests/
|   |-- __init__.py
|   |-- test_etl.py
|-- .gitignore
|-- README.md
|-- requirements.txt
```

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd pyspark-pipeline
    ```

2.  **Create a virtual environment (optional but recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3.  **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  **Place your raw CSV files in the `data/raw` directory.**

    A sample CSV file is provided in `data/raw/sample_data.csv`.

2.  **Run the ETL pipeline:**
    ```bash
    python src/main.py
    ```

3.  **The processed data will be saved in the `data/processed` directory in both Parquet and Delta formats.**

## Running Tests

To run the unit tests, run the following command from the root of the project:

```bash
python -m unittest tests/test_etl.py
```
