import requests
import pandas as pd
import psycopg2
from datetime import datetime

# Step 1: Extract Data from API
def extract_data():
    try:
        # Fetch COVID-19 data for all countries from the API
        url = "https://disease.sh/v3/covid-19/countries"
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for invalid responses
        data = response.json()  # Get the data in JSON format
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from the API: {e}")
        return []

# Step 2: Transform the Data
def transform_data(data):
    # Convert the data into a pandas DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # Select relevant columns and drop any unnecessary ones
    df = df[['country', 'cases', 'deaths', 'recovered', 'active', 'population']]
    
    # Add current date to the DataFrame for each record
    df['date'] = datetime.now().date()

    # Return the transformed DataFrame
    return df

# Step 3: Load Data into PostgreSQL
def load_data_to_postgresql(df):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="covid_data",  # your database name
            user="postgres",  # your PostgreSQL username
            password="nopassword123",  # your PostgreSQL password
            host="localhost",  # or the host you're using
            port="5432"  # default PostgreSQL port
        )
        cursor = conn.cursor()

        # Insert data into PostgreSQL table
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO covid_data (country, date, cases, deaths, recovered, active, population)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['country'], row['date'], row['cases'], row['deaths'], row['recovered'], row['active'], row['population']))

        # Commit the transaction to save data
        conn.commit()
        print(f"Successfully loaded {len(df)} records into PostgreSQL.")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
    finally:
        # Close the connection
        cursor.close()
        conn.close()

# ETL Process
def run_etl():
    print("Starting ETL process...")
    
    # Extract data
    data = extract_data()
    if not data:
        print("No data to process. Exiting ETL.")
        return

    # Transform data
    transformed_data = transform_data(data)

    # Load data into PostgreSQL
    load_data_to_postgresql(transformed_data)

    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()


