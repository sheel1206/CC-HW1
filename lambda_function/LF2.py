import json
import boto3
import requests
import random
from requests.auth import HTTPBasicAuth

# AWS Clients
sqs = boto3.client("sqs")
ses = boto3.client("ses")
dynamodb = boto3.resource("dynamodb")
credentials = boto3.Session().get_credentials()


auth = HTTPBasicAuth("sheel1206", "Sp@12062001")
# AWS Resource Details
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/703671915892/dining-requests"
OPENSEARCH_URL = "https://search-restaurant-search-vc2jqnpgtvs224vplp3kuafkqi.us-east-1.es.amazonaws.com"
DYNAMODB_TABLE_NAME = "yelp-restaurants"
SENDER_EMAIL = "sheel126x@gmail.com"  # Replace with verified SES email

def lambda_handler(event, context):

    # Poll SQS for new messages
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,  
        WaitTimeSeconds=10,  
        VisibilityTimeout=10  
    )

    if "Messages" not in response or len(response["Messages"]) == 0:
        print("No messages in SQS.")
        return {"statusCode": 200, "body": "No messages found."}

    for message in response["Messages"]:

        try:
            # Parse user request
            user_request = json.loads(message["Body"])
        except json.JSONDecodeError:
            print("Error: Message is not valid JSON.")
            continue

        cuisine = user_request.get("cuisine", "Unknown Cuisine")
        dining_time = user_request.get("time", "Unknown Time")
        num_people = user_request.get("people", "Unknown People")
        email = user_request.get("email", "Unknown Email")

        load_cuisine_records_to_opensearch(cuisine, limit=20)

        business_ids = query_opensearch(cuisine)
        if not business_ids:
            print("No restaurants found in OpenSearch.")
            continue
        
        detailed_restaurants = fetch_restaurant_details(business_ids)

        send_email(email, detailed_restaurants, cuisine, dining_time, num_people)

        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])

    return {"statusCode": 200, "body": "LF2 successfully processed messages."}

def load_cuisine_records_to_opensearch(cuisine, limit=20):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    try:
        response = table.scan(
            FilterExpression="cuisine = :cuisine_val",
            ExpressionAttributeValues={":cuisine_val": cuisine}
        )
        restaurants = response.get("Items", [])
        if len(restaurants) > limit:
            restaurants = random.sample(restaurants, limit)

        print(f"📡 Sending {len(restaurants)} {cuisine} restaurants to OpenSearch.")

        for restaurant in restaurants:
            business_id = restaurant.get("business_id")  
            cuisine_type = restaurant.get("cuisine")  

            if not business_id or not cuisine_type:
                print(f"Skipping restaurant with missing business_id or cuisine: {restaurant}")
                continue

            index_data = {
                "business_id": business_id,
                "cuisine": cuisine_type
            }
            send_to_opensearch(business_id, index_data)

    except Exception as e:
        print(f"Error scanning DynamoDB for {cuisine}: {e}")

def send_to_opensearch(business_id, data):
    url = f"{OPENSEARCH_URL}/restaurants/_doc"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, auth=auth, headers=headers, json=data)
        print(f"OpenSearch Index Response: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"Error sending data to OpenSearch: {e}")

def query_opensearch(cuisine):
    url = f"{OPENSEARCH_URL}/restaurants/_search"
    query = {
        "query": {
            "match": {
                "cuisine": cuisine  
            }
        },
        "size": 5  
    }
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(url, auth=auth, headers=headers, json=query)  # ✅ Added auth
        print(f"📡 OpenSearch Query Response: {response.status_code} - {response.text}")

        if response.status_code == 401:
            print("Authentication failed. Check credentials or OpenSearch policy.")

        data = response.json()
        if "hits" in data and "hits" in data["hits"]:
            return [hit["_source"]["business_id"] for hit in data["hits"]["hits"]]
    except Exception as e:
        print(f"OpenSearch Query Error: {e}")

    return []

def fetch_restaurant_details(business_ids):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    restaurants = []

    for bid in business_ids:
        try:
            response = table.get_item(Key={"business_id": bid})  
            if "Item" in response:
                restaurants.append(response["Item"])
        except Exception as e:
            print(f"Error fetching {bid} from DynamoDB: {e}")

    return restaurants


def send_email(email, restaurants, cuisine, dining_time, num_people):
    email_body = f"Hello! Here are my {cuisine} restaurant suggestions for {num_people} people at {dining_time}:\n\n"

    for r in restaurants:
        email_body += f"{r['name']} - {r['address']}, Rating: {r['rating']}\n"

    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": f"{cuisine} Restaurant Recommendations"},
                "Body": {"Text": {"Data": email_body}}
            }
        )
        print(f"Email sent to {email}")

    except Exception as e:
        print(f" Error sending email: {e}")
