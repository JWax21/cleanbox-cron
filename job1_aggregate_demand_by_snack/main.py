from pymongo import MongoClient
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
connection_string = "mongodb+srv://josh:xZFxm7owlIqr07Ai@cleanbox-dev.grpgk4b.mongodb.net/?retryWrites=true&w=majority&appName=CLEANBOX-DEV"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create a MongoClient
client = MongoClient(connection_string)

print("Connecting to MongoDB:", connection_string)

try:
    db = client["Boxes"]
    draftboxes_collection = db["draftboxes"]
    customers_collection = db["customers"]

    pipeline = [
        {"$match": {"month": 625}},
        {
            "$lookup": {
                "from": "customers",
                "localField": "customerID",
                "foreignField": "customerID",
                "as": "customer_data"
            }
        },
        {"$unwind": "$customer_data"},
        {"$unwind": "$snacks"},
        {
            "$addFields": {
                "order_status": "$customer_data.order_status"
            }
        },
        {
            "$group": {
                "_id": "$snacks.SnackID",
                "confirmed": {
                    "$sum": {
                        "$cond": [{"$eq": ["$order_status", "Confirmed"]}, 1, 0]
                    }
                },
                "pending": {
                    "$sum": {
                        "$cond": [{"$ne": ["$order_status", "Confirmed"]}, 1, 0]
                    }
                }
            }
        },
        {
            "$addFields": {
                "projected": {"$add": ["$confirmed", "$pending"]}
            }
        },
        {"$sort": {"projected": -1}}
    ]

    results = list(draftboxes_collection.aggregate(pipeline))

    print("Demand breakdown by snackID (month = 625):")
    for r in results:
        print(f"sku: {r['_id']} | confirmed: {r['confirmed']} | pending: {r['pending']} | projected: {r['projected']}")

    # Prepare for Supabase insert
    data_to_insert = [
        {
            "sku": r["_id"],
            "confirmed": r["confirmed"],
            "pending": r["pending"],
            "projected": r["projected"]
        }
        for r in results
    ]

    response = supabase.table("fd_aggregate_demand_by_snack").upsert(data_to_insert).execute()

    if response.data:
        print(f"Successfully inserted {len(response.data)} records into Supabase.")
    else:
        print("No records inserted into Supabase.")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    client.close()