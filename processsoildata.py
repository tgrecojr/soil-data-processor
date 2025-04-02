
import fieldmappings
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import glob
import psycopg2

global_vars = {}

def formatdate(date,time):
    localdatetime = date + time
    date_time_obj = datetime.strptime(localdatetime, '%Y%m%d%H%M')
    return date_time_obj

def removevaluesnotrecorded(original_value):
    if (original_value == -99.000 or original_value == -9999.0 or original_value == -9999):
        return None
    else:
        return original_value

def converttofarenheit(celsius_value):
    if celsius_value is not None:
        return (celsius_value * 9/5) + 32
    else:
        return celsius_value


def processdata():

    start_time = datetime.now()
    print("Retreiving list of files to processs")
    
    files = glob.glob(global_vars["SOIL_DATA_LOCATION"])
    
    for f in files:
        print("Processing file: {}".format(f))
        df = pd.read_fwf(
            f,
            colspecs=fieldmappings.colspecs, 
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True,
            skip_blank_lines=True)

        df['UTC_DATETIME'] = df.apply(lambda row: formatdate(row['UTC_DATE'],row['UTC_TIME']), axis=1)
        df['LOCAL_DATETIME'] = df.apply(lambda row: formatdate(row['LST_DATE'],row['LST_TIME']), axis=1)
        df['SOIL_TEMP_5'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_5'])),axis=1)
        df['SOIL_TEMP_10'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_10'])),axis=1)
        df['SOIL_TEMP_20'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_20'])),axis=1)
        df['SOIL_TEMP_50'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_50'])),axis=1)
        df['SOIL_TEMP_100'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_100'])),axis=1)
        df['T_CALC'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['T_CALC'])),axis=1)
        df['T_HR_AVG'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['T_HR_AVG'])),axis=1)
        df['P_CALC'] = df.apply(lambda row: removevaluesnotrecorded(row['P_CALC']),axis=1)
        df['RH_HR_AVG'] = df.apply(lambda row: removevaluesnotrecorded(row['RH_HR_AVG']),axis=1)
        df = df.set_index(['UTC_DATETIME'])
        df = df.replace({np.nan: None})

        connection = "postgres://{}:{}@{}:5432/{}".format(global_vars["SOIL_DATABASE_USER"],global_vars["SOIL_DATABASE_PASSWORD"],global_vars["SOIL_DATABASE_HOST"],global_vars["SOIL_DATABASE"])
        insert_query = """
           INSERT INTO soil_data (time, SOIL_TEMP_5, SOIL_TEMP_10,SOIL_TEMP_20,SOIL_TEMP_50,SOIL_TEMP_100,T_CALC, T_HR_AVG,P_CALC,RH_HR_AVG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           """
        with psycopg2.connect(connection) as conn:
            cursor = conn.cursor()
            for index, row in df.iterrows():
                data = (row['LOCAL_DATETIME'], row['SOIL_TEMP_5'],row['SOIL_TEMP_10'],row['SOIL_TEMP_20'],row['SOIL_TEMP_50'],row['SOIL_TEMP_100'],row['T_CALC'],row['T_HR_AVG'],row['P_CALC'],row['RH_HR_AVG'])
                try:
                    cursor.execute(insert_query, data)
                except psycopg2.errors.UniqueViolation:
                    pass
                conn.commit()
            cursor.close()
      
    print("Data inserted into the database successfully.")
   
    end_time = datetime.now()
    time_taken = end_time - start_time
    print(f"Processed Soil Temps in {time_taken}")
   
def load_env_vars(var_names):
    for var_name in var_names:
        var_value = os.environ.get(var_name)
        if var_value is not None:
            global global_vars
            global_vars[var_name] = var_value
        else:
            print(f"Environment variable '{var_name}' not found.")
            sys.exit(1)

def create_table_if_not_exists():
   create_glucose_table_query = """
                  CREATE TABLE soil_data (
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
   create_glucose_hypertable_query = "SELECT create_hypertable('solidata', by_range('time'));"
   connection = "postgres://{}:{}@{}:5432/{}".format(global_vars["SOIL_DATABASE_USER"],global_vars["SOIL_DATABASE_PASSWORD"],global_vars["SOIL_DATABASE_HOST"],global_vars["SOIL_DATABASE"])
   with psycopg2.connect(connection) as conn:
      cursor = conn.cursor()
      try:
         cursor.execute(create_glucose_table_query)
         conn.commit()
      except psycopg2.errors.DuplicateTable:
         conn.rollback()
      try:
         cursor.execute(create_glucose_hypertable_query)
         conn.commit()
      except psycopg2.DatabaseError:
         conn.rollback()
      cursor.close()


def main():
    
    env_vars_to_load = ["SOIL_DATA_LOCATION", "SOIL_DATABASE", "SOIL_DATABASE_USER","SOIL_DATABASE_PASSWORD","SOIL_DATABASE_HOST"]
    load_env_vars(env_vars_to_load)
    create_table_if_not_exists()
    processdata()

if __name__ == "__main__":
    main()