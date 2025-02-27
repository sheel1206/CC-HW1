import json
import boto3

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/703671915892/dining-requests"

def lambda_handler(event, context):
    print("Lex Event Received:", json.dumps(event, indent=4))

    # Extract intent and slots from Lex V2 event structure
    intent_name = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent'].get('slots', {})

    location = slots.get("Location", {}).get("value", {}).get("interpretedValue", None)
    cuisine = slots.get("Cuisine", {}).get("value", {}).get("interpretedValue", None)
    dining_time = slots.get("Time", {}).get("value", {}).get("interpretedValue", None)
    people = slots.get("People", {}).get("value", {}).get("interpretedValue", None)
    email = slots.get("Email", {}).get("value", {}).get("interpretedValue", None)

    # If Lex didn't send slots, return a debugging message
    if not slots:
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Failed"}
            },
            "messages": [{"contentType": "PlainText", "content": "No slot data received from Lex."}]
        }

    # Create message payload for SQS
    message_body = {
        "location": location,
        "cuisine": cuisine,
        "time": dining_time,
        "people": people,
        "email": email
    }

    # Send message to SQS
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message_body)
    )

    print("Sent to SQS:", message_body)

    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {"name": intent_name, "state": "Fulfilled"}
        },
        "messages": [{"contentType": "PlainText", "content": f"Thank you! I will find {cuisine} restaurants in {location} for {people} people at {dining_time}. You will receive an email at {email} soon."}]
    }
