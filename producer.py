# pip install google-cloud-pubsub go to the command prompt and then paste the command 
from google.cloud import pubsub_v1  
import glob
import os
import csv
import json

#Search the current directory for the JSON file (including the service account key)
#to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

 #Set the project_id with your project ID
project_id = "spatial-encoder-448821-q6"
topic_name = "Milestone2SQL"

#create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing records from CSV to {topic_path}.")

def change_value(x):
    try:
        if "." in x:  # If the number has a decimal, convert to float
            return float(x)
        return int(x)  # if not, convert to int
    except ValueError:
        return x  # If the conversion fails, return as string


# read and publish CSV
csv_file = "Labels.csv"
with open(csv_file, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        # convert into JSON
        message = json.dumps(row).encode("utf-8")
        
        #send the value
        print(f"Producing record: {message}")
        future = publisher.publish(topic_path, message)
        future.result() 

print("All records published.")