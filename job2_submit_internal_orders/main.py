import os
import logging
from pymongo import MongoClient
from datetime import datetime
import pytz
from supabase import create_client, Client

# ----------------------------
# Configure logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# Load environment variables
# ----------------------------
logging.info("Loading environment variables...")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
connection_string = os.getenv("MONGO_CONNECTION_STRING")

if not SUPABASE_URL or not SUPABASE_KEY or not connection_string:
    logging.error("Missing required environment variables.")
    exit(1)

# ----------------------------
# Initialize Supabase client
# ----------------------------
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client initialized.")
except Exception as e:
    logging.exception("Failed to initialize Supabase client.")
    exit(1)

# ----------------------------
# Connect to MongoDB
# ----------------------------
try:
    client = MongoClient(connection_string)
    db = client['Boxes']
    draftboxes_collection = db['draftboxes']
    customers_collection = db['customers']
    logging.info("Connected to MongoDB and collections initialized.")
except Exception as e:
    logging.exception("Failed to connect to MongoDB.")
    exit(1)

# ----------------------------
# Get next month (Eastern Time)
# ----------------------------
try:
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    if now.month == 12:
        next_month = 1
        year = now.year + 1
    else:
        next_month = now.month + 1
        year = now.year

    current_month = int(f"{next_month:02d}{year % 100:02d}")
    logging.info(f"Targeting next month (Eastern Time): {current_month}")
except Exception as e:
    logging.exception("Failed to determine next Eastern month.")
    exit(1)

# ----------------------------
# Query draftboxes
# ----------------------------
try:
    draftboxes_cursor = draftboxes_collection.find({"month": current_month})
    logging.info("Fetched draftboxes for target month.")
except Exception as e:
    logging.exception("Failed to fetch draftboxes.")
    exit(1)

records_to_insert = []

for draftbox in draftboxes_cursor:
    customer_id = draftbox.get("customerID")
    if not customer_id:
        logging.warning("Skipping draftbox with missing customerID.")
        continue

    try:
        customer = customers_collection.find_one({
            "customerID": customer_id,
            "stripe_status": {"$in": ["active", "trialing"]}
        })
        if not customer:
            logging.info(f"Skipping customer {customer_id}: not active or trialing.")
            continue
    except Exception as e:
        logging.exception(f"Error fetching customer {customer_id}")
        continue

    snacks = draftbox.get("snacks", [])
    total_snacks = 0

    for snack in snacks:
        count = snack.get("count", 1)
        total_snacks += count

    record = {
        "customer_id": customer_id,
        "name": f"{customer.get('firstName', '').strip()} {customer.get('lastName', '').strip()}".strip(),
        "email": customer.get("email"),
        "subscription_type": customer.get("subscription_type"),
        "month": draftbox.get("month"),
        "total_snacks": total_snacks
    }

    logging.info(f"Prepared record for customer: {customer_id}")
    records_to_insert.append(record)

# ----------------------------
# Output records for review
# ----------------------------
logging.info(f"Prepared {len(records_to_insert)} records.")
for rec in records_to_insert:
    print(rec)

# ----------------------------
# Insert into Supabase
# ----------------------------
for rec in records_to_insert:
    try:
        supabase.table("fd_internal_orders").insert(rec).execute()
        logging.info(f"Inserted record for {rec['customer_id']}")
    except Exception as e:
        logging.exception(f"Failed to insert record for {rec['customer_id']}")