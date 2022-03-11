
import fieldmappings
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import numpy as np
import os
import sys
import glob

def formatdate(date,time):
    localdatetime = date + time
    date_time_obj = datetime.strptime(localdatetime, '%Y%m%d%H%M')
    return date_time_obj

def removevaluesnotrecorded(original_value):
    if (original_value == -99.000 or original_value == -9999.0 or original_value == -9999):
        return np.nan
    else:
        return original_value

def converttofarenheit(celsius_value):
    if celsius_value is not np.nan:
        return (celsius_value * 9/5) + 32
    else:
        return celsius_value


def processdata():

    start_time = datetime.now()
    
    SOIL_INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
    SOIL_INFLUX_ORG = os.environ["INFLUX_ORG"]
    SOIL_INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]
    SOIL_INFLUX_URL = os.environ["INFLUX_URL"]
    SOIL_INFLUX_BATCH_SIZE = os.environ["INFLUX_BATCH_SIZE"]
    SOIL_DATA_LOCATION = os.environ['SOIL_DATA_LOCATION']
    SOIL_DATA_GLOB = "/ftp.ncdc.noaa.gov/pub/data/uscrn/products/hourly02/**/CRNH0203-????-*.txt"

    influx_client = InfluxDBClient(url=SOIL_INFLUX_URL, token=SOIL_INFLUX_TOKEN, org=SOIL_INFLUX_ORG)
    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS,batch_size=SOIL_INFLUX_BATCH_SIZE, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000)


    print("Retreiving list of files to processs")
    files = glob.glob(SOIL_DATA_LOCATION + SOIL_DATA_GLOB)
    for f in files:
        print("Processing file: {}".format(f))
        df = pd.read_fwf(
            f,
            colspecs=fieldmappings.colspecs, 
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True)

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
        
        selected_columns = df[["LOCAL_DATETIME","SOIL_TEMP_5","SOIL_TEMP_10","SOIL_TEMP_20","SOIL_TEMP_50","SOIL_TEMP_100","T_CALC","T_HR_AVG","WBANNO"]]
        slim_df  = selected_columns.copy()
        
        influx_write_api.write(SOIL_INFLUX_BUCKET, record=slim_df,data_frame_measurement_name='soildata',data_frame_tag_columns=['WBANNO'])

    end_time = datetime.now()
    time_taken = end_time - start_time
    print(f"Processed Soil Temps in {time_taken}")

    
def main():
    
    try:  
        os.environ['INFLUX_TOKEN']
        os.environ['INFLUX_ORG']
        os.environ['INFLUX_BUCKET']
        os.environ['INFLUX_URL']
        os.environ['INFLUX_BATCH_SIZE']
        os.environ['SOIL_DATA_LOCATION']
    except KeyError: 
        print("Not all environment variables have been set.")
        print("Please set INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, INFLUX_URL, INFLUX_BATCH_SIZE, SOIL_DATA_LOCATION")
        sys.exit(1)
    
    processdata()

if __name__ == "__main__":
    main()