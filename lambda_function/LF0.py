import boto3
import json
import uuid

lex = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        body = {}
        if "body" in event and event["body"]:
            if isinstance(event["body"], str):
                body = json.loads(event["body"])
            elif isinstance(event["body"], dict):
                body = event["body"]
        user_message = None
        if "messages" in body and isinstance(body["messages"], list):
            first_message = body["messages"][0]  
            if "unstructured" in first_message and "text" in first_message["unstructured"]:
                user_message = first_message["unstructured"]["text"]

        if not user_message:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'text' in request body"})
            }

        user_id = body.get("userId", "default-session")  

        # Call Amazon Lex
        response = lex.recognize_text(
            botId="VHR3NGATDK",  
            botAliasId="HVH47UKMYL",
            localeId="en_US",
            sessionId=user_id,
            text=user_message
        )

        lex_message = response.get("messages", [{}])[0].get("content", "Lex didn't return a message.")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                            "text": lex_message
                                    }
                }
                ]
                }


    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format in request body"})
        }
    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
