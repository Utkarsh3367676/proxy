import time
import secrets
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from pymongo import MongoClient

# -----------------------------
# Config (replace with your values or load from env)
# -----------------------------
TENANT_ID = 
CLIENT_ID = 
CLIENT_SECRET =
SUBSCRIPTION_ID = 

MONGO_URI = "mongodb://daasbillingdb:hm9KEpDArKn4grHaWaKyLAjJwpTFvvZlanNIXr3EYFFHEwu7wT7jCrAHiyIaYh1AvTtcfhdpSJrfACDb8bWirg%3D%3D@daasbillingdb.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@daasbillingdb@" 
DB_NAME = "daas-db"
COLLECTION_NAME = "storage-accounts"

TAG_KEY = "daas-deployment"
TAG_VALUE = "dev"


# -----------------------------
# MongoDB Helpers
# -----------------------------
def generate_storage_id():
    return "storage_" + secrets.token_urlsafe(16)


def storage_exists(collection, account_name):
    return collection.find_one({"AccountName": account_name}) is not None


def insert_storage(collection, account_name):
    document = {
        "_id": generate_storage_id(),
        "DateCreated": int(time.time()),
        "DateModified": None,
        "CustomerId": "",   
        "TenantId": "default",
        "AccountName": account_name,
        "CpId": "",     
        "StorageType": 0
    }
    collection.insert_one(document)
    print(f"Inserted new Storage Account: {account_name}")


# -----------------------------
# Azure Logic
# -----------------------------
def get_storage_accounts():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    resource_client = ResourceManagementClient(credential, SUBSCRIPTION_ID)

    print("Fetching Storage Accounts with tag "
          f"{TAG_KEY}={TAG_VALUE} ...")

    storage_accounts = []
    for resource in resource_client.resources.list(
        filter=f"resourceType eq 'Microsoft.Storage/storageAccounts'"
    ):
        if resource.tags and resource.tags.get(TAG_KEY) == TAG_VALUE:
            storage_accounts.append(resource.name)

    print(f"Found {len(storage_accounts)} matching storage accounts")
    return storage_accounts


# -----------------------------
# Main Runner
# -----------------------------
if __name__ == "__main__":
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Get all Azure SAs by tag
    sa_list = get_storage_accounts()

    # Insert into MongoDB if not present
    for sa in sa_list:
        if not storage_exists(collection, sa):
            insert_storage(collection, sa)
        else:
            print(f"Storage Account {sa} already exists, skipping")
