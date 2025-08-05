import json
from pymongo import MongoClient
import os

# === CONFIGURAZIONI ===
MONGO_URI = "mongodb+srv://sampledb:NutriNow1@nutrinow.b870xoi.mongodb.net/ "
DB_NAME = "glovo_db"
COLLECTION_RESTAURANTS = "glovo_restaurants"
COLLECTION_DISHES = "glovo_dishes"

# === FILE PATHS ===
DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data/processed')
RESTAURANTS_FILE = os.path.join(DATA_DIR, 'ristoranti_glovo_clean.json')
DISHES_FILE = os.path.join(DATA_DIR, 'piatti_calorie_allergeni.json')

# === FUNZIONE DI CARICAMENTO ===
def load_json_to_mongo(file_path, db, collection_name):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    if isinstance(data, list):
        # Bulk insert
        if data:
            db[collection_name].delete_many({})  # Optional: Pulisce la collection prima
            db[collection_name].insert_many(data)
            print(f"Caricati {len(data)} documenti in '{collection_name}'")
        else:
            print(f"Nessun documento da caricare in '{collection_name}'")
    elif isinstance(data, dict):
        db[collection_name].delete_many({})
        db[collection_name].insert_one(data)
        print(f"Caricato 1 documento in '{collection_name}'")
    else:
        print(f"Formato JSON non riconosciuto per {file_path}")

# === MAIN ===
if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    load_json_to_mongo(RESTAURANTS_FILE, db, COLLECTION_RESTAURANTS)
    load_json_to_mongo(DISHES_FILE, db, COLLECTION_DISHES)

    client.close()
