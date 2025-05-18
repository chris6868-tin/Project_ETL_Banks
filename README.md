# ETL Pipeline for Largest Banks Data

This project implements an ETL (Extract, Transform, Load) pipeline to scrape data about the world's largest banks from Wikipedia, transform market capitalization values into multiple currencies, and load the results into a CSV file and a PostgreSQL database. It also allows users to run custom SQL queries on the data.

# Project tasks
Task 1:
Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. Use the list of log points provided to create log entries as every stage of the code.

Task 2:
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
b. Write the code for a function extract() to perform the required data extraction.
c. Execute a function call to extract() to verify the output.

Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
a. Write the code for a function transform() to perform the said task.
b. Execute a function call to transform() and verify the output.

Task 4:
Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.

Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.

Task 6:
Run queries on the database table. Write a function load_to_db(), execute a given set of queries and verify the output.

Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt.

## Features
- **Extract**: Scrapes bank data (name and market capitalization in USD) from a Wikipedia page.
- **Transform**: Converts market capitalization into GBP, EUR, and INR using exchange rates from a CSV file.
- **Load**: Saves data to a CSV file and a PostgreSQL database.
- **Query**: Supports running custom SQL queries on the database.
- **Logging**: Logs all pipeline steps to a file for debugging.

## Project Structure
Project_ETL_Banks/
├── src/
│   └── project_banks.py        # Main ETL script
├── data/
│   └── exchange_rate.csv       # Exchange rate data
├── logs/
│   └── log_code.txt            # Log file
├── output/
│   └── Largest_banks_data.csv  # Output CSV
├── .env                        # Environment variables
├── README.md                   # Project documentation
├── requirements.txt            # Dependencies
├── .gitignore                  # Git ignore file
└── LICENSE                     # License

## Prerequisites
- Python 3.8+
- PostgreSQL server running on `localhost:5432`
- Required Python libraries (see `requirements.txt`)

## Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/Project_ETL_Banks.git
   cd Project_ETL_Banks
   ```
2. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3. **Set up PostgreSQL**:
    Create a database named etl_db2:
    ```bash
    createdb -U postgres etl_db2
    ```

    Configure database credentials in .env:
    ```env
    DB_USER=postgres
    DB_PASS=your_password
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=etl_db2
    ```
4. **Prepare exchange rate data**:
    Ensure data/exchange_rate.csv exists with the following format:
    ```csv
    Currency,Rate
    GBP,0.75
    EUR,0.85
    INR,83.5
    ```
## Usage
1. **Run the ETL pipeline**:
    ```bash
    python src/project_banks.py
    ``` 
2. **Follow prompts to enter an SQL query (e.g., SELECT * FROM largest_banks) or press Enter to skip**.
    Outputs:
        CSV file: output/Largest_banks_data.csv
        Database table: largest_banks in etl_db2
        Log file: logs/log_code.txt
    Example Query
    ```sql
    SELECT * FROM largest_banks WHERE MC_USD_Billion > 1000;
    ```