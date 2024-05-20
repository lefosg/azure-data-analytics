from azure.storage.blob import BlobServiceClient
import sys
import os
import time
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

def upload_to_azure_blob(storage_connection_string, container_name, file_path):
    try:
        # Create BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(file_path))

        if blob_client.exists():
            blob_client.delete_blob()

        print(f"Uploading file '{file_path}' to Azure Blob Storage...")

        # Upload the file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        print("Upload completed successfully.")

    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {e}")

async def streambatches():
# Create a producer client to send messages to the event hub.
# Specify a connection string to your event hubs namespace and
# the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="event_hub_key", eventhub_name="minibatch")
    async with producer:
    # Create a batch.
        event_data_batch = await producer.create_batch()
        # Add events to the batch.
        path = "batches"
        dir_list = os.listdir(path)
        for file in dir_list:
            print("Sending " + file)
            f = open(path+"/"+file, mode="r").read()
            event_data_batch.add(EventData([file, f]))
        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

# Azure Blob Storage connection string
connection_string = "blob_key"

# Name of the container in Azure Blob Storage
container_name = "vault"

choice = sys.argv[1]

if choice == 'single':
    # Call the function to upload the file to Azure Blob Storage
    # File path of the file to upload
    upload_to_azure_blob(connection_string, container_name, sys.argv[2])
elif choice == 'batch':
    # Pull the files from a directory with already existing file batches
    # Stream with frequency of 1 seconds
    # Call the function to upload the file to Azure Blob Storage
    #path = "batches"
    #dir_list = os.listdir(path)
    #for file in dir_list:
    #    time.sleep(1)
    #    upload_to_azure_blob(connection_string, container_name, path+"/"+file)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(streambatches())
else:
    print("Enter 'single filename'  for file upload or 'batch' for streaming")

