# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c3f79ae5-7804-40e1-8a77-b065eabe7413",
# META       "default_lakehouse_name": "LH_RealTimeFlightStatus",
# META       "default_lakehouse_workspace_id": "7e5444cf-f6c4-420a-9d25-5e86efa12407",
# META       "known_lakehouses": [
# META         {
# META           "id": "c3f79ae5-7804-40e1-8a77-b065eabe7413"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import pandas as pd
import logging
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Logging**

# CELL ********************

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **API Setup**

# CELL ********************

API_KEY = "bdb2cd76-dfe5-4286-a7a1-2d50ec6eff67"
BASE_URL = "http://airlabs.co/api/v9/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fetch API Data**

# CELL ********************

def fetch_api_data(endpoint):
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", params={"api_key": API_KEY})
        response.raise_for_status()
        data = response.json().get("response", [])
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching {endpoint}: {e}")
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fetch Data**

# CELL ********************

df_flights = fetch_api_data("flights")
df_airlines = fetch_api_data("airlines")
df_airports = fetch_api_data("airports")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_airlines.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Data Cleaning**

# CELL ********************

def clean_flights(df):
    required_cols = ['flight_icao','dep_icao','arr_icao','airline_icao','lat','lng','status']
    return df.dropna(subset=required_cols)

def clean_airlines(df):
    return df.dropna(subset=['icao_code','name']).drop_duplicates(subset='icao_code')

def clean_airports(df):
    return df.dropna(subset=['icao_code','name','lat','lng']).drop_duplicates(subset='icao_code')

df_flights_clean = clean_flights(df_flights)
df_airlines_clean = clean_airlines(df_airlines)
df_airports_clean = clean_airports(df_airports)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add Timestamp**

# CELL ********************

from datetime import datetime, timezone
df_flights_clean.loc[:,"ingestion_time"] = datetime.now(timezone.utc)
#print(df_flights_clean["ingestion_time"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save to Lakehouse**

# CELL ********************

#Conversion of pandas dataframes to spark dataframes
spark_df_flights=spark.createDataFrame(df_flights_clean)
spark_df_airlines=spark.createDataFrame(df_airlines_clean)
spark_df_airports=spark.createDataFrame(df_airports_clean)
#writing the dataframes to tables in onelake
spark_df_flights.write.mode('overwrite').saveAsTable("flights_silver")
spark_df_airlines.write.mode('overwrite').saveAsTable("airlines_silver")
spark_df_airports.write.mode('overwrite').saveAsTable("airports_silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Layer**

# CELL ********************

df_gold = spark.sql("""
SELECT 
    f.lat AS flight_latitude,
    f.lng AS flight_longitude,
    f.flight_icao,
    f.arr_icao,
    f.dep_icao,
    f.ingestion_time,
    a_arr.name AS arrival_airport_name,
    a_dep.name AS departure_airport_name,
    al.name AS airline_name,
    f.status
FROM flights_silver f
LEFT JOIN airports_silver a_arr ON a_arr.icao_code = f.arr_icao
LEFT JOIN airports_silver a_dep ON a_dep.icao_code = f.dep_icao
LEFT JOIN airlines_silver al ON al.icao_code = f.airline_icao
LIMIT 200
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save gold data**

# CELL ********************

df_gold.write.mode("overwrite").option("mergeSchema","true").saveAsTable("flights_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
