# import csv

# # argv[1]: lines of output.csv
# # argv[2]: number of batches
# # argv[3]: lines per batch

# def clean_dataset(input_file, output_file):
#     cleaned_data = []
#     # batch info
#     num_batches = int(sys.argv[2])
#     batch_size = int(sys.argv[3])
#     batches_created = 1
#     # create directory for batches if it doesn't exist
#     exists = os.path.exists('batches')
#     if not exists:
#         os.makedirs('batches')
    
#     header = []

#     with open(input_file, 'r') as csv_file:
#         csv_reader = csv.reader(csv_file)

#         for row in csv_reader:
#             # check if any column has value 0 or is empty
#             if '0' in row or '' in row:
#                 continue
#             cleaned_data.append(row)

#             # if creating the big file
#             if csv_reader.line_num == int(sys.argv[1]):
#                 print("Writing to " + output_file)
#                 with open(output_file, 'w', newline='') as csv_output:
#                     csv_writer = csv.writer(csv_output)
#                     csv_writer.writerows(cleaned_data)
#                 header = cleaned_data[0]
#                 cleaned_data = []
#             # if creating batches
#             elif csv_reader.line_num > int(sys.argv[1]):
#                 # if number of batches created is the one wanted, end
#                 if batches_created == num_batches:
#                     return
#                 # if batch is read, store it
#                 if csv_reader.line_num == int(sys.argv[1]) + batches_created*batch_size:
#                     cleaned_data.insert(0, header)
#                     print("Writing to " + output_file)
#                     with open("batches/batch"+str(batches_created)+".csv", 'w', newline='') as csv_output:
#                         csv_writer = csv.writer(csv_output)
#                         csv_writer.writerows(cleaned_data)
#                     cleaned_data = []
#                     batches_created += 1

# clean_dataset('train.csv', 'output.csv')

import sys
import os
import pandas as pd

def process_csv(input_file, outputsize, batchsize, batchnumbers):
    df = pd.read_csv(input_file, nrows=outputsize+batchsize*batchnumbers+10)

    # Extract the first 'outputsize' lines and store them in 'output.csv'
    df_output = df.head(outputsize)
    df_output = df_output[~df_output.apply(lambda row: row.isin(['', '0', '0.0', 0, 0.0]).any(), axis=1)]
    df_output.to_csv('output.csv', index=False)
    
    # Reset the dataframe to read the file from the beginning
    all_batches = df.tail(batchsize*batchnumbers+10)
    
    exists = os.path.exists('batches')
    if not exists:
        os.makedirs('batches')
    # Create batches of size 'batchsize' and store them in separate files
    for i in range(0, batchnumbers):
        batch = all_batches[i*batchsize:(i+1)*batchsize]
        batch = batch[~batch.apply(lambda row: row.isin(['', '0', '0.0', 0, 0.0]).any(), axis=1)]
        batch.to_csv(f'batches/batch_{i+1}.csv', index=False)

# Specify the input file and parameters
input_file = 'train.csv'
outputsize = int(sys.argv[1])  # Specify the desired output size
batchnumbers = int(sys.argv[2])  # Specify the number of batches to create
batchsize = int(sys.argv[3])  # Specify the desired batch size

# Call the function to process the CSV file
process_csv(input_file, outputsize, batchsize, batchnumbers)