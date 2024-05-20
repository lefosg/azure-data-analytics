import azure.functions as func
from azure.storage.blob import  BlobServiceClient
import logging
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import redis
from tempfile import TemporaryFile
from io import StringIO

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

redis_hostname = "taxi.redis.cache.windows.net"
redis_key = "redis_key"

redis_con = redis.StrictRedis(host=redis_hostname, port=6380,
                      password=redis_key, ssl=True)

@app.blob_trigger(arg_name="myblob", path="vault/{name}.csv",
                               connection="AzureWebJobsStorage") 
def BlobTrigger(myblob: func.InputStream):
    """
    Blob trigger function. This is spins off the processing of a file uploaded.
    """
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    df = pd.read_csv(myblob)
    processQueries(df)

@app.route(route="HttpExample")
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function. This is for retrieving processed data from Redis cache (quick and easy visualization).
    """
    logging.info('Python HTTP trigger function processed a request.')
    quarter1 = int(redis_con.get("Q1_1"))
    quarter2 = int(redis_con.get("Q1_2"))
    quarter3 = int(redis_con.get("Q1_3"))
    quarter4 = int(redis_con.get("Q1_4"))
    #Q2 = str(redis_con.get("Q2"))
    #Q3 = str(redis_con.get("Q3"))
    #Q5 = str(redis_con.get("Q5"))
    quarter1_stream = int(redis_con.get("Q4_1"))
    quarter2_stream = int(redis_con.get("Q4_2"))
    quarter3_stream = int(redis_con.get("Q4_3"))
    quarter4_stream = int(redis_con.get("Q4_4"))
    return func.HttpResponse(f"Query 1"
                             f"\nquarter_1 = {quarter1}"
                             f"\nquarter_2 = {quarter2}"
                             f"\nquarter_3 = {quarter3}"
                             f"\nquarter_4 = {quarter4}"
                             #f"\n----------------------"
                             #f"\nQuery 2 "
                             #f"\n{Q2}"
                             #f"\n----------------------"
                             #f"\nQuery 3 "
                             #f"\n{Q3}"
                             #f"\n----------------------"
                             #f"\nQuery 5 "
                             #f"\n{Q5}"
                             f"\n----------------------"
                             f"\nStreaming"
                             f"\nquarter_1 = {quarter1_stream}"
                             f"\nquarter_2 = {quarter2_stream}"
                             f"\nquarter_3 = {quarter3_stream}"
                             f"\nquarter_4 = {quarter4_stream}")
    
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="minibatch",
                               connection="EventHubString") 
def eventhub_trigger(azeventhub: func.EventHubEvent):
    """
    Event trigger function. This is for streaming (mini-batch processing)
    """
    x = azeventhub.get_body().decode('utf-8').split('csv', 1)
    filename = x.pop(0)
    csvStringIO = StringIO(x.pop(0))
    df = pd.read_csv(csvStringIO, sep=",")
    del x
    (_, q1, q2, q3, q4) = Q1(df)
    try:
        q1 += int(redis_con.get("Q4_1")) 
        q2 += int(redis_con.get("Q4_2")) 
        q3 += int(redis_con.get("Q4_3")) 
        q4 += int(redis_con.get("Q4_4"))
        data = [['Q1_1', q1], ['Q1_2', q2], ['Q1_3', q3], ['Q1_4', q4]]
        quarters_stream = pd.DataFrame(data, columns=['Quarters', 'Values'])
        upload_to_azure_blob("blob_key",
                            "results", quarters_stream, "q1_stream.csv")
    except: 
        logging.error("Could not fetch Q4_i vars from redis")    
    redis_con.set("Q4_1", q1)
    redis_con.set("Q4_2", q2)
    redis_con.set("Q4_3", q3)
    redis_con.set("Q4_4", q4)

def upload_to_azure_blob(storage_connection_string:str, container_name:str, df:pd.DataFrame, filename:str):
    try:
        output = df.to_csv(filename, encoding="utf-8",sep=',')
        print(type(output))
        print(output)
        # fp = TemporaryFile('w+t')
        # fp.write(df.to_string())

        # # Go back to the beginning and read data from file
        # fp.seek(0)
        # data = fp.read()

        # Create BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

        if blob_client.exists():
            blob_client.delete_blob()

        print(f"Uploading file {filename} to Azure Blob Storage...")

        # Upload the file
        with open(filename, "rb") as data:
            blob_client.upload_blob(data)
        
        # Close the file, after which it will be removed
        # fp.close()

        print("Upload completed successfully.")

    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {e}")

def processQueries(df):
    #Q1
    df_1,q1_count,q2_count,q3_count,q4_count = Q1(df)
    data = [['Q1_1', q1_count], ['Q1_2', q2_count], ['Q1_3', q3_count], ['Q1_4', q4_count]]
    quarters = pd.DataFrame(data, columns=['Quarters', 'Values'])
    upload_to_azure_blob("blob_key",
                         "results", quarters, "query1_analytics.csv")
    #Q2
    df_2 = Q2(df_1)
    upload_to_azure_blob("blob_key",
                         "results", df_2, "query2_analytics.csv")
    #Q3
    result = Q3(df_2)
    upload_to_azure_blob("blob_key",
                         "results", result, "query3_analytics.csv")
    #Q5
    count_per_hour = Q5(df,40.721319,-73.844311,5,10)
    upload_to_azure_blob("blob_key",
                         "results", count_per_hour, "query5_analytics.csv")

    # Set Values to REDIS
    redis_con.set("Q1_1", str(q1_count))
    redis_con.set("Q1_2", str(q2_count))
    redis_con.set("Q1_3", str(q3_count))
    redis_con.set("Q1_4", str(q4_count))
    #redis_con.set("Q2", str(df_2))
    #redis_con.set("Q3", str(result))
    #redis_con.set("Q5", str(count_per_hour))   

#Q1
def Q1(df):
    q1_count, q2_count, q3_count, q4_count = 0, 0, 0, 0
    center_latitude = 40.735923
    center_longitude = -73.990294

    for index, row in df.iterrows():
        longitude = row[3]
        latitude = row[4]
        if latitude >= center_latitude and longitude >= center_longitude:
            q1_count += 1
            df['Quarter'] = 1
        elif latitude >= center_latitude and longitude < center_longitude:
             q4_count += 1
             df['Quarter'] = 4
        elif latitude < center_latitude and longitude >= center_longitude:
            q2_count += 1
            df['Quarter'] = 2
        else:
            q3_count += 1
            df['Quarter'] = 3
    return df,q1_count,q2_count,q3_count,q4_count

#Q2
def Q2(df):
    new_df = pd.DataFrame(columns=df.columns)
    for index, row in df.iterrows():
        lat1 = row['pickup_latitude']
        long1 = row['pickup_longitude']
        lat2 = row['dropoff_latitude']
        long2 = row['dropoff_longitude']
        dist_km = haversine(lat1, long1, lat2, long2)
        if row[7] > 2 and row[1] > 10 and dist_km > 1:
            new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)
    print(new_df)
    return new_df

def haversine(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):
    if to_radians:
        lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    a = np.sin((lat2-lat1)/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2
    return earth_radius * 2 * np.arcsin(np.sqrt(a))

#Q3
def Q3(df):

    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['pickup_hour'] = df['pickup_datetime'].dt.hour

    # Compute the most popular pickup hours
    top_pickup_hours = df['pickup_hour'].value_counts().head(5).index.tolist()
    print(top_pickup_hours)
    # Filter the DataFrame to include only rows with pickup hours in top_pickup_hours
    top_pickup_df = df[df['pickup_hour'].isin(top_pickup_hours)]

    result = top_pickup_df.groupby(['pickup_hour', 'Quarter'])['passenger_count'].sum().reset_index()
    result = result.sort_values(by=['pickup_hour', 'passenger_count'], ascending=[True, False])
    result = result.groupby('pickup_hour').first()
    return result

#Q5
def Q5(df, lat_center, lon_center, rad, fare):
    pickup_lon_col = "pickup_longitude"
    pickup_lat_col = "pickup_latitude"
    faire_col = "fare_amount"
    rides = []

    # iterating over rows using iterrows() function
    for _, row in df.iterrows():
        current_pickup_lat = row[pickup_lat_col]
        current_pickup_lon = row[pickup_lon_col]
        fare_amount = float(row[faire_col])

        if haversine(lat_center, lon_center, current_pickup_lat, current_pickup_lon) <= rad and fare_amount < fare:
            rides.append(row)
    
    rides_df = pd.DataFrame(rides, columns=["key","fare_amount","pickup_datetime","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count"])
    rides_df['pickup_datetime'] = pd.to_datetime(rides_df['pickup_datetime'])
    
    count_per_hour = rides_df.groupby(rides_df["pickup_datetime"].dt.hour).size().reset_index(name='count')
    count_per_hour.columns = ['hour', 'count']
    return count_per_hour


def haversine_5(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return (c * r).__trunc__()


