import requests
import boto3
import json
import time

# Yelp API Key (Replace with your actual API Key)
API_KEY = "yu0yKYkpSXyjQzwSQsebI7crbYTgRxRtvVmF-vHerX8fDrYYEdhzh2sIr2wyVwn7hX05_0XmA8Ql04cGKOhPIpBi9SkeYI0Mum5zJoJXzHgSs9c7t8mOEfobdP29Z3Yx"

# Define Yelp API endpoint
YELP_API_URL = "https://api.yelp.com/v3/businesses/search"

# Headers for authentication
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# List of cuisines and city
cuisines = ["Chinese", "Italian", "Mexican", "Japanese", "Indian", "Mediterranian", "Thai", "American"]  # Add more cuisines
city = "Manhattan, NY"

# AWS DynamoDB Setup
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("yelp-restaurants")  # Make sure this table exists in AWS

def get_restaurants(cuisine, city):
    """Fetch up to 1,000 restaurant results for a cuisine in a city, avoiding API limits"""
    all_restaurants = []
    offset = 0
    limit = 50  # Yelp allows a max of 50 results per request

    while offset < 1000:  # Yelp's max limit per search is 1,000
        params = {
            "term": f"{cuisine} restaurants",
            "location": city,
            "limit": limit,
            "offset": offset  # Pagination
        }

        response = requests.get(YELP_API_URL, headers=HEADERS, params=params)

        if response.status_code == 200:
            data = response.json().get("businesses", [])
            if not data:
                break  # Stop if no more data
            
            all_restaurants.extend(data)
            print(f"Fetched {len(data)} restaurants for {cuisine} in {city} (offset {offset})")
        else:
            print(f"Error fetching {cuisine} in {city}: {response.json()}")
            break  # Stop on error

        offset += 1  # Move to next set of results
        time.sleep(1)  # Prevent hitting Yelp API rate limits

    return all_restaurants

def store_in_dynamodb(restaurants, cuisine):
    """Stores restaurant data in DynamoDB while avoiding duplicates"""
    for restaurant in restaurants:
        try:
            business_id = restaurant["id"]

            # Check if restaurant already exists
            existing_item = table.get_item(Key={"business_id": business_id})
            if "Item" in existing_item:
                print(f"Skipping duplicate: {restaurant['name']}")
                continue  # Skip if already in database

            item = {
                "business_id": business_id,
                "name": restaurant["name"],
                "address": ", ".join(restaurant["location"]["display_address"]),
                "latitude": str(restaurant["coordinates"]["latitude"]),
                "longitude": str(restaurant["coordinates"]["longitude"]),
                "num_reviews": restaurant["review_count"],
                "rating": str(restaurant["rating"]),
                "zip_code": restaurant["location"].get("zip_code", "Unknown"),
                "cuisine": cuisine,
                "insertedAtTimestamp": str(time.time())
            }

            # Store in DynamoDB
            table.put_item(Item=item)
            print(f"Stored: {restaurant['name']} ({business_id})")

        except Exception as e:
            print(f"Error inserting {restaurant['name']}: {e}")

# Fetch and store data for all cuisines & cities
for cuisine in cuisines:
    #for city in cities:
        print(f"Fetching {cuisine} restaurants in {city}...")
        restaurants = get_restaurants(cuisine, city)
        store_in_dynamodb(restaurants, cuisine)

print("✅ All restaurants have been fetched and stored!")
