This repo includes all the code for an Azure function app which processes the dataset in this repo. Datasets are uploaded to a Blob Storage. The Azure function Blob trigger fire up, and do some calculations on that specific dataset.
Streaming of mini-batches is also supported through an Event Hub which is set up. The Even Hub Trigger starts handling the mini-batch stream.
- preprocess_dataset.py x1 x2 x3: gets the first x1 lines from the dataset and produces another csv called output.csv (locally). After that, it  creates x2 batches of x3 lines each. Empirically, due to limitations of Event Hub, 20 lines per mini-batch csv file is the maximum size supported.
- upload.py: supports two oprations 'upload.py single name_of_file.csv' which uploads the dataset to the Blob Storage (and the Blob trigger is invoked), and 'upload.py batch' which pulls the minibatch files from a directory created by preprocess_dataset.py and streams them to the Event Hub.
- function_appy.py: code that is executed in the azure function. Includes 4 queries for the dataset uploaded, and the two triggers (Blob and Event Hub)

  
Results of the calculations are stored to Blob Storage, and some specific results are saved to Redis Cache. 
