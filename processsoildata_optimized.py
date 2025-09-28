import fieldmappings
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import glob
import psycopg2
import psycopg2.extras

global_vars = {}


def formatdate_vectorized(dates, times):
    """Vectorized date formatting using pandas datetime functions"""
    combined = dates.astype(str) + times.astype(str).str.zfill(4)
    return pd.to_datetime(combined, format='%Y%m%d%H%M')


def remove_invalid_values_vectorized(series):
    """Vectorized removal of invalid values"""
    invalid_values = [-99.000, -9999.0, -9999]
    return series.replace(invalid_values, np.nan)


def celsius_to_fahrenheit_vectorized(celsius_series):
    """Vectorized temperature conversion"""
    return celsius_series * 9/5 + 32


def process_dataframe_optimized(df):
    """Optimized dataframe processing using vectorized operations"""

    # Vectorized datetime creation
    df["UTC_DATETIME"] = formatdate_vectorized(df["UTC_DATE"], df["UTC_TIME"])
    df["LOCAL_DATETIME"] = formatdate_vectorized(df["LST_DATE"], df["LST_TIME"])

    # Define temperature columns for batch processing
    temp_columns = ["SOIL_TEMP_5", "SOIL_TEMP_10", "SOIL_TEMP_20", "SOIL_TEMP_50", "SOIL_TEMP_100", "T_CALC", "T_HR_AVG"]

    # Vectorized processing for all temperature columns at once
    for col in temp_columns:
        df[col] = celsius_to_fahrenheit_vectorized(remove_invalid_values_vectorized(df[col]))

    # Process non-temperature columns
    df["P_CALC"] = remove_invalid_values_vectorized(df["P_CALC"])
    df["RH_HR_AVG"] = remove_invalid_values_vectorized(df["RH_HR_AVG"])

    # Set index and handle NaN values
    df = df.set_index(["UTC_DATETIME"])
    df = df.replace({np.nan: None})

    return df


def bulk_insert_optimized(df, connection_string, batch_size=1000):
    """Optimized bulk database insertion"""

    # Prepare data for bulk insert
    columns = ["time", "SOIL_TEMP_5", "SOIL_TEMP_10", "SOIL_TEMP_20", "SOIL_TEMP_50",
               "SOIL_TEMP_100", "T_CALC", "T_HR_AVG", "P_CALC", "RH_HR_AVG"]

    data_columns = ["LOCAL_DATETIME", "SOIL_TEMP_5", "SOIL_TEMP_10", "SOIL_TEMP_20",
                   "SOIL_TEMP_50", "SOIL_TEMP_100", "T_CALC", "T_HR_AVG", "P_CALC", "RH_HR_AVG"]

    # Convert to list of tuples for bulk insert
    data_tuples = [tuple(row[col] for col in data_columns) for _, row in df.iterrows()]

    with psycopg2.connect(connection_string) as conn:
        cursor = conn.cursor()

        # Use execute_values for high-performance bulk insert
        insert_query = """
            INSERT INTO soildata (time, SOIL_TEMP_5, SOIL_TEMP_10, SOIL_TEMP_20, SOIL_TEMP_50,
                                 SOIL_TEMP_100, T_CALC, T_HR_AVG, P_CALC, RH_HR_AVG)
            VALUES %s
            ON CONFLICT (time) DO NOTHING
        """

        # Process in batches to manage memory
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            try:
                psycopg2.extras.execute_values(
                    cursor,
                    insert_query,
                    batch,
                    template=None,
                    page_size=batch_size
                )
                conn.commit()
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            except Exception as e:
                print(f"Error inserting batch {i//batch_size + 1}: {e}")
                conn.rollback()

        cursor.close()


def processdata():
    start_time = datetime.now()
    print("Retrieving list of files to process")

    files = glob.glob(global_vars["SOIL_DATA_LOCATION"])
    connection_string = "postgres://{}:{}@{}:5432/{}".format(
        global_vars["SOIL_DATABASE_USER"],
        global_vars["SOIL_DATABASE_PASSWORD"],
        global_vars["SOIL_DATABASE_HOST"],
        global_vars["SOIL_DATABASE"],
    )

    total_records = 0

    for f in files:
        file_start = datetime.now()
        print(f"Processing file: {f}")

        # Read file with optimized pandas settings
        df = pd.read_fwf(
            f,
            colspecs=fieldmappings.colspecs,
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True,
            skip_blank_lines=True,
            on_bad_lines="skip",
        )

        print(f"Loaded {len(df)} records from {f}")

        # Process dataframe with optimized functions
        df = process_dataframe_optimized(df)

        # Bulk insert with batching
        bulk_insert_optimized(df, connection_string, batch_size=1000)

        total_records += len(df)
        file_end = datetime.now()
        file_time = file_end - file_start
        print(f"Processed {f} in {file_time} ({len(df)} records)")

    print(f"Data inserted into the database successfully. Total records: {total_records}")

    end_time = datetime.now()
    time_taken = end_time - start_time
    print(f"Processed Soil Temps in {time_taken}")


def load_env_vars(var_names):
    for var_name in var_names:
        var_value = os.environ.get(var_name)
        if var_value is not None:
            global_vars[var_name] = var_value
        else:
            print(f"Environment variable '{var_name}' not found.")
            sys.exit(1)


def create_table_if_not_exists():
    create_soil_table_query = """
                  CREATE TABLE IF NOT EXISTS soildata (
                  time TIMESTAMPTZ NOT NULL,
                  SOIL_TEMP_5 INTEGER,
                  SOIL_TEMP_10 float8,
                  SOIL_TEMP_20 float8,
                  SOIL_TEMP_50 float8,
                  SOIL_TEMP_100 float8,
                  T_CALC float8,
                  T_HR_AVG DECIMAL,
                  P_CALC float8,
                  RH_HR_AVG DECIMAL,
                  PRIMARY KEY(time))
                  """
    connection = "postgres://{}:{}@{}:5432/{}".format(
        global_vars["SOIL_DATABASE_USER"],
        global_vars["SOIL_DATABASE_PASSWORD"],
        global_vars["SOIL_DATABASE_HOST"],
        global_vars["SOIL_DATABASE"],
    )
    with psycopg2.connect(connection) as conn:
        cursor = conn.cursor()
        cursor.execute(create_soil_table_query)
        conn.commit()
        cursor.close()


def main():
    env_vars_to_load = [
        "SOIL_DATA_LOCATION",
        "SOIL_DATABASE",
        "SOIL_DATABASE_USER",
        "SOIL_DATABASE_PASSWORD",
        "SOIL_DATABASE_HOST",
    ]
    load_env_vars(env_vars_to_load)
    create_table_if_not_exists()
    processdata()


if __name__ == "__main__":
    main()