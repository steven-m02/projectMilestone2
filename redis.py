from google.cloud import pubsub_v1 
import glob  
import base64  
import os  

# search JSON
service_account_files = glob.glob("*.json")
if not service_account_files:
    raise FileNotFoundError("No service account JSON file found in the current directory.")
# connect google cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_files[0]

#  project id and topic name
project_id = "spatial-encoder-448821-q6"  
topic_name = "Milestone2Redis" 

# pub sub
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing messages with ordering keys to {topic_path}.")

# get images from same directory
image_folder = "." 
image_files = glob.glob(os.path.join(image_folder, "*.png"))  

if not image_files:
    print("No image files found in the current directory.")
    exit()

# put file to Pub/Sub
for image_path in image_files:
    key = os.path.basename(image_path)  

    try:
        # Read and encode the image in Base64
        with open(image_path, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

        
        future = publisher.publish(topic_path, encoded_image.encode("utf-8"), ordering_key=key)
        future.result()  
        print(f"Successfully published: {key}")

    except Exception as e:
        print(f"Failed to publish {key}: {e}")

print("All messages have been published successfully.")
