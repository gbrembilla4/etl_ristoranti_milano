import json
from pymongo import MongoClient

# === CONFIGURAZIONI ===
MONGO_URI = "mongodb+srv://sampledb:NutriNow1@nutrinow.b870xoi.mongodb.net/"
DB_NAME = "ristoranti_milano"
COLLECTION_RESTAURANTS = "glovo_restaurants"
COLLECTION_DISHES = "glovo_dishes"

# === FILE PATHS ===
RESTAURANTS_FILE = "/Users/gloria.brembilla/Documents/AIDA/Modulo1/02 - BI/etl_ristoranti_milano/data/exports/ristoranti_clean.json"
DISHES_FILE = "/Users/gloria.brembilla/Documents/AIDA/Modulo1/02 - BI/etl_ristoranti_milano/data/exports/piatti_con_allergeni_aggiornati.json"

# === FUNZIONE DI CARICAMENTO ===
def load_json_to_mongo(file_path, db, collection_name):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    if isinstance(data, list):
        if data:
            db[collection_name].delete_many({})  # Pulisce la collection prima di inserire
            db[collection_name].insert_many(data)
            print(f"✅ Caricati {len(data)} documenti in '{collection_name}'")
        else:
            print(f"⚠ Nessun documento da caricare in '{collection_name}'")
    elif isinstance(data, dict):
        db[collection_name].delete_many({})
        db[collection_name].insert_one(data)
        print(f"✅ Caricato 1 documento in '{collection_name}'")
    else:
        print(f"❌ Formato JSON non riconosciuto per {file_path}")

# === MAIN ===
if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    load_json_to_mongo(RESTAURANTS_FILE, db, COLLECTION_RESTAURANTS)
    load_json_to_mongo(DISHES_FILE, db, COLLECTION_DISHES)

    client.close()
