import fieldmappings
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import glob
import psycopg2
import psycopg2.extras
import structlog

global_vars = {}

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO level
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger("soil_data_processor")


def formatdate_vectorized(dates, times):
    """Vectorized date formatting using pandas datetime functions with error handling"""
    # Convert to string and handle missing/empty values
    dates_str = dates.astype(str).str.strip()
    times_str = times.astype(str).str.strip()

    # Handle time padding more carefully - only pad if it's a valid number
    def safe_zfill(time_str):
        if time_str == '' or time_str == 'nan' or not time_str.isdigit():
            return time_str
        return time_str.zfill(4)

    times_str = times_str.apply(safe_zfill)

    # Create combined datetime strings
    combined = dates_str + times_str

    # Only mark as invalid if clearly empty or 'nan'
    # Don't reject '0000' time as it's a valid midnight time
    invalid_mask = (dates_str == '') | (dates_str == 'nan') | (times_str == '') | (times_str == 'nan')
    combined = combined.mask(invalid_mask, '')

    # Convert to datetime with error handling - invalid dates become NaT (Not a Time)
    return pd.to_datetime(combined, format='%Y%m%d%H%M', errors='coerce')


def remove_invalid_values_vectorized(series):
    """Vectorized removal of invalid values with string-to-numeric conversion"""
    # First convert to numeric, handling any string values
    series_numeric = pd.to_numeric(series, errors='coerce')

    # Define invalid values
    invalid_values = [-99.000, -9999.0, -9999]

    # Replace invalid values with NaN
    return series_numeric.replace(invalid_values, np.nan)


def celsius_to_fahrenheit_vectorized(celsius_series):
    """Vectorized temperature conversion"""
    return celsius_series * 9/5 + 32


def process_dataframe_optimized(df):
    """Optimized dataframe processing using vectorized operations"""

    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Vectorized datetime creation
    df["UTC_DATETIME"] = formatdate_vectorized(df["UTC_DATE"], df["UTC_TIME"])
    df["LOCAL_DATETIME"] = formatdate_vectorized(df["LST_DATE"], df["LST_TIME"])

    # Analyze datetime issues before removing rows
    initial_count = len(df)
    utc_invalid = df["UTC_DATETIME"].isna().sum()
    local_invalid = df["LOCAL_DATETIME"].isna().sum()

    if utc_invalid > 0 or local_invalid > 0:
        logger.warning(
            "Datetime parsing issues detected",
            utc_invalid_count=utc_invalid,
            local_invalid_count=local_invalid,
            total_rows=initial_count
        )

        # Show some examples of problematic data
        if utc_invalid > 0:
            bad_utc = df[df["UTC_DATETIME"].isna()][["UTC_DATE", "UTC_TIME"]].head(3)
            logger.debug(
                "Sample UTC datetime parsing failures",
                examples=bad_utc.to_dict('records')
            )

        if local_invalid > 0:
            bad_local = df[df["LOCAL_DATETIME"].isna()][["LST_DATE", "LST_TIME"]].head(3)
            logger.debug(
                "Sample local datetime parsing failures",
                examples=bad_local.to_dict('records')
            )

    # Remove rows with invalid datetime values (NaT - Not a Time)
    df = df.dropna(subset=["UTC_DATETIME", "LOCAL_DATETIME"])
    final_count = len(df)

    if initial_count != final_count:
        removed_count = initial_count - final_count
        removal_percentage = removed_count/initial_count*100
        logger.warning(
            "Removed rows with invalid datetime values",
            removed_count=removed_count,
            removal_percentage=round(removal_percentage, 1),
            valid_records=final_count,
            initial_count=initial_count
        )

    # Define temperature columns for batch processing
    temp_columns = ["SOIL_TEMP_5", "SOIL_TEMP_10", "SOIL_TEMP_20", "SOIL_TEMP_50", "SOIL_TEMP_100", "T_CALC", "T_HR_AVG"]

    # Vectorized processing for all temperature columns at once
    for col in temp_columns:
        df.loc[:, col] = celsius_to_fahrenheit_vectorized(remove_invalid_values_vectorized(df[col]))

    # Process non-temperature columns
    df.loc[:, "P_CALC"] = remove_invalid_values_vectorized(df["P_CALC"])
    df.loc[:, "RH_HR_AVG"] = remove_invalid_values_vectorized(df["RH_HR_AVG"])

    # Set index and handle NaN values
    df = df.set_index(["UTC_DATETIME"])
    df = df.replace({np.nan: None})

    return df


def bulk_insert_optimized(df, connection_string, batch_size=1000):
    """Optimized bulk database insertion"""

    # Prepare data for bulk insert
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
                logger.info(
                    "Batch inserted successfully",
                    batch_number=i//batch_size + 1,
                    batch_size=len(batch),
                    total_batches=(len(data_tuples) + batch_size - 1) // batch_size
                )
            except Exception as e:
                logger.error(
                    "Error inserting batch",
                    batch_number=i//batch_size + 1,
                    error=str(e),
                    batch_size=len(batch)
                )
                conn.rollback()

        cursor.close()


def processdata():
    start_time = datetime.now()

    files = glob.glob(global_vars["SOIL_DATA_LOCATION"])
    connection_string = "postgres://{}:{}@{}:5432/{}".format(
        global_vars["SOIL_DATABASE_USER"],
        global_vars["SOIL_DATABASE_PASSWORD"],
        global_vars["SOIL_DATABASE_HOST"],
        global_vars["SOIL_DATABASE"],
    )

    logger.info(
        "Starting soil data processing",
        file_pattern=global_vars["SOIL_DATA_LOCATION"],
        files_found=len(files),
        database_host=global_vars["SOIL_DATABASE_HOST"],
        database_name=global_vars["SOIL_DATABASE"]
    )

    total_records = 0

    for f in files:
        file_start = datetime.now()
        file_logger = logger.bind(file_path=f)
        file_logger.info("Processing file started")

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

        file_logger.info("File loaded", initial_records=len(df))

        # Process dataframe with optimized functions
        df = process_dataframe_optimized(df)

        # Bulk insert with batching
        bulk_insert_optimized(df, connection_string, batch_size=1000)

        total_records += len(df)
        file_end = datetime.now()
        file_time = file_end - file_start

        file_logger.info(
            "File processing completed",
            processing_time=str(file_time),
            processed_records=len(df),
            cumulative_records=total_records
        )

    logger.info(
        "All soil data processing completed",
        total_files_processed=len(files),
        total_records_inserted=total_records,
        total_processing_time=str(datetime.now() - start_time)
    )


def load_env_vars(var_names):
    missing_vars = []
    for var_name in var_names:
        var_value = os.environ.get(var_name)
        if var_value is not None:
            global_vars[var_name] = var_value
            logger.debug("Environment variable loaded", variable=var_name)
        else:
            missing_vars.append(var_name)

    if missing_vars:
        logger.error(
            "Required environment variables not found",
            missing_variables=missing_vars
        )
        sys.exit(1)

    logger.info(
        "Environment variables loaded successfully",
        loaded_variables=list(global_vars.keys())
    )


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

    logger.info("Creating database table if not exists", table_name="soildata")

    try:
        with psycopg2.connect(connection) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_soil_table_query)
                conn.commit()
                logger.info("Database table creation completed successfully")
            except psycopg2.errors.DuplicateTable:
                conn.rollback()
                logger.info("Database table already exists")
            cursor.close()
    except Exception as e:
        logger.error(
            "Failed to create database table",
            error=str(e),
            database_host=global_vars["SOIL_DATABASE_HOST"],
            database_name=global_vars["SOIL_DATABASE"]
        )
        raise


def main():
    logger.info("Soil Data Processor starting", version="2.0", features=["optimized_processing", "structured_logging", "bulk_inserts"])

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

    logger.info("Soil Data Processor completed successfully")



# Individual functions for backward compatibility with tests
def formatdate(date_str, time_str):
    """Convert date and time strings to datetime object."""
    import pandas as pd

    # Convert to pandas Series for vectorized function
    dates = pd.Series([date_str])
    times = pd.Series([time_str])
    result = formatdate_vectorized(dates, times)
    return result.iloc[0] if not pd.isna(result.iloc[0]) else None


def removevaluesnotrecorded(value):
    """Remove invalid values, returning None for invalid values."""
    import pandas as pd

    # Convert to pandas Series for vectorized function
    series = pd.Series([value])
    result = remove_invalid_values_vectorized(series)
    return result.iloc[0] if not pd.isna(result.iloc[0]) else None


def converttofarenheit(celsius):
    """Convert Celsius to Fahrenheit."""
    import pandas as pd

    if celsius is None:
        return None

    # Convert to pandas Series for vectorized function
    series = pd.Series([celsius])
    result = celsius_to_fahrenheit_vectorized(series)
    return result.iloc[0] if not pd.isna(result.iloc[0]) else None


if __name__ == "__main__":
    main()
