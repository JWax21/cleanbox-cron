from pymongo import MongoClient
from supabase import create_client, Client
from dotenv import load_dotenv
import os


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
connection_string = os.getenv("MONGO_CONNECTION_STRING")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create a MongoClient
client = MongoClient(connection_string)

print("Connecting to MongoDB:", connection_string)

try:
    # Access the 'Boxes' database
    db = client['Boxes']

    # Access the 'draftboxes' and 'customers' collections
    draftboxes_collection = db['draftboxes']
    customers_collection = db['customers']

    # Aggregation pipeline to count and sort by frequency
    pipeline = [
        # Match draftboxes with month = 625
        {"$match": {"month": 625}},
        # Lookup to join with customers collection
        {
            "$lookup": {
                "from": "customers",
                "localField": "customerID",
                "foreignField": "customerID",
                "as": "customer_data"
            }
        },
        # Filter for active customers
        {
            "$match": {
                "customer_data.stripe_status": "active"
            }
        },
        # Unwind the snacks array
        {"$unwind": "$snacks"},
        # Group by snackID and count
        {"$group": {"_id": "$snacks.SnackID", "count": {"$sum": 1}}},
        # Sort by count in descending order
        {"$sort": {"count": -1}}
    ]

    # Execute aggregation and collect results
    results = list(draftboxes_collection.aggregate(pipeline))

    # Print results
    print("Count of each snackID in 'draftboxes' collection (month = 625, active customers, sorted by frequency):")
    for result in results:
        print(f"snackID: {result['_id']}, Count: {result['count']}")

    # Prepare data for Supabase
    data_to_insert = [{"SKU": result["_id"], "COUNT": result["count"]} for result in results]

    # Insert results into Supabase table (e.g., 'Demand_Aggregate')
    response = supabase.table("Demand_Aggregate").upsert(data_to_insert).execute()

    # Check if the insert was successful
    if response.data:
        print(f"Successfully inserted {len(response.data)} records into Supabase.")
    else:
        print("No records inserted into Supabase.")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close the MongoDB connection
    client.close()