import pandas as pd
import json
import yaml
import os
import re
from bson import ObjectId
from unidecode import unidecode
import logging
from datetime import datetime
from collections import defaultdict
import hashlib

def load_config():
    """Carica la configurazione"""
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
ETL_CONFIG = load_config()

# === CONFIGURAZIONE LOGGING ===
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "main_etl.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Inizio processo ETL Glovo")

# === CONFIGURAZIONI ===
DATA_QUALITY = ETL_CONFIG["data_quality"]
VERSIONE_ETL = ETL_CONFIG["etl"]["versione"]
MILAN = ETL_CONFIG["milan"]
CLASSIFICATIONS = ETL_CONFIG["classifications"]

# === 1. CARICA DATI ORIGINALI ===
try:
    input_file = os.path.join(os.path.dirname(__file__), "..", "..", ETL_CONFIG["file_paths"]["raw_data"])
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    logger.info(f"Caricati {len(raw_data)} ristoranti dal file: {input_file}")
except FileNotFoundError:
    logger.error(f"File '{input_file}' non trovato")
    exit(1)

# === 2. FUNZIONI DI UTILITA' ===

def validate_milan_address(address):
    """Verifica se l'indirizzo è effettivamente a Milano"""
    if not address:
        return False
    address_lower = address.lower()

    keywords = MILAN["keywords"] + MILAN["caps"] + MILAN["zones"]
    return any(keyword in address_lower for keyword in keywords)


def normalize_restaurant_name(name):
    """Standardizza nomi ristoranti simili"""
    if not name:
        return ""
    
    name = name.strip()
    
    # Standardizzazioni comuni
    replacements = {
        r"mc\s*donald'?s?": "McDonald's",
        r"burger\s*king": "Burger King",
        r"pizza\s*express": "Pizza Express",
        r"sushi\s*daily": "Sushi Daily",
        r"old\s*wild\s*west": "Old Wild West",
        r"&": "e",
        r"\s+": " "  # Spazi multipli → singolo
    }
    
    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)
    
    return name.strip()

def clean_address(address):
    """Pulizia indirizzi"""
    if not address:
        return ""
    
    # Rimuovi caratteri speciali e normalizza
    address = unidecode(str(address)).strip()
    
    # Standardizzazioni comuni
    address = re.sub(r'\s+', ' ', address)  # Spazi multipli
    address = re.sub(r'[,]{2,}', ',', address)  # Virgole multiple
    
    # Standardizza abbreviazioni
    replacements = {
        r'\bv\.?\s*': 'Via ',
        r'\bp\.?\s*za\s*': 'Piazza ',
        r'\bc\.?\s*so\s*': 'Corso ',
        r'\bv\.?\s*le\s*': 'Viale ',
        r'\blargo\s*': 'Largo ',
    }
    
    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    
    return address

def validate_price(price):
    """Validazione prezzi"""
    if price is None or price == "":
        return 0.0, False
    try:
        cleaned = re.sub(r'[^\d.,]', '', str(price)).replace(',', '.')
        price_float = float(cleaned)
        is_valid = DATA_QUALITY["min_price"] <= price_float <= DATA_QUALITY["max_price"]
        return price_float, is_valid
    except ValueError:
        return 0.0, False

def clean_ingredients(ingr):
    """Pulizia ingredienti"""
    if isinstance(ingr, list):
        ingredients = [i.strip() for i in ingr if i and i.strip()]
    elif isinstance(ingr, str):
        ingredients = [i.strip() for i in ingr.split(',') if i and i.strip()]
    else:
        return []
    
    # Filtra ingredienti non validi
    invalid_patterns = [
        r"ingredienti non disponibili",
        r"non specificat[io]",
        r"da definire",
        r"^-+$",
        r"^\s*$"
    ]
    
    valid_ingredients = []
    for ingredient in ingredients:
        is_valid = True
        for pattern in invalid_patterns:
            if re.search(pattern, ingredient.lower()):
                is_valid = False
                break
        
        if is_valid and len(ingredient) >= 2:
            valid_ingredients.append(ingredient)
    
    return valid_ingredients

def has_valid_ingredients(ingredients):
    """Verifica se il piatto ha ingredienti validi"""
    return len(ingredients) > 0

def validate_dish_name(nome):
    """Valida il nome del piatto"""
    if not nome or not nome.strip():
        return False, "Nome vuoto"
    nome = nome.strip()

    if len(nome) < DATA_QUALITY["min_dish_name_length"]:
        return False, "Nome troppo corto"
    if len(nome) > DATA_QUALITY["max_dish_name_length"]:
        return False, "Nome troppo lungo"
    if re.match(r'^[\d\s\-_]+$', nome):
        return False, "Nome non valido (solo numeri/simboli)"

    return True, "OK"


def classify_dish_type(nome_piatto, nome_ristorante, ingredienti):
    """Classifica il tipo di piatto"""
    nome_piatto_lower = nome_piatto.lower()
    nome_ristorante_lower = nome_ristorante.lower()
    ingredienti_str = " ".join([i.lower() for i in ingredienti])

    scores = defaultdict(int)
    for category, keywords in CLASSIFICATIONS.items():
        for kw in keywords["restaurant_keywords"]:
            if kw in nome_ristorante_lower:
                scores[category] += 3
        for kw in keywords["dish_keywords"]:
            if kw in nome_piatto_lower:
                scores[category] += 5
        for kw in keywords["ingredient_keywords"]:
            if kw in ingredienti_str:
                scores[category] += 2

    if scores:
        return max(scores.items(), key=lambda x: x[1])[0]
    return "altro"


def generate_restaurant_hash(nome, indirizzo):
    """Genera hash per identificare ristoranti duplicati"""
    # Normalizza per il confronto
    nome_norm = normalize_restaurant_name(nome).lower()
    indirizzo_norm = clean_address(indirizzo).lower()
    
    # Rimuovi numeri civici per confronto più flessibile
    indirizzo_base = re.sub(r'\d+', '', indirizzo_norm).strip()
    
    combined = f"{nome_norm}|{indirizzo_base}"
    return hashlib.md5(combined.encode()).hexdigest()

def validate_opening_hours(orari):
    """Valida e standardizza gli orari"""
    if not orari or not isinstance(orari, list):
        return []
    
    validated_hours = []
    for orario in orari:
        if isinstance(orario, str) and orario.strip():
            # Standardizza formato orari
            orario_clean = re.sub(r'[\s\-–—]+', '-', orario.strip())
            orario_clean = re.sub(r'(\d{1,2}):(\d{2})', r'\1:\2', orario_clean)
            validated_hours.append(orario_clean)
    
    return validated_hours

def convert_objectid(obj):
    """Converte ObjectId in string"""
    return str(obj) if isinstance(obj, ObjectId) else obj

# === 3. STATISTICHE E CONTROLLI QUALITA' ===
quality_stats = {
    "ristoranti_processati": 0,
    "ristoranti_scartati": 0,
    "ristoranti_duplicati": 0,
    "piatti_processati": 0,
    "piatti_scartati": 0,
    "piatti_prezzo_anomalo": 0,
    "indirizzi_non_milano": 0,
    "motivi_scarto": defaultdict(int)
}

# === 4. PROCESSO PRINCIPALE DI PULIZIA ===
ristoranti_clean = []
piatti_clean = []
seen_restaurants = {}  # Hash → restaurant data per controllo duplicati
tipo_piatto_stats = defaultdict(int)

logger.info("Inizio processo di pulizia dati")

for i, r in enumerate(raw_data):
    quality_stats["ristoranti_processati"] += 1
    
    # Validazioni base
    if not r.get("nome") or not r.get("indirizzo"):
        quality_stats["ristoranti_scartati"] += 1
        quality_stats["motivi_scarto"]["nome_o_indirizzo_mancante"] += 1
        continue
    
    nome_originale = r["nome"].strip()
    indirizzo_originale = r["indirizzo"]
    
    # Normalizza nome ristorante
    nome_ristorante = normalize_restaurant_name(nome_originale)
    
    # Pulisci indirizzo
    indirizzo_pulito = clean_address(indirizzo_originale)
    
    # Valida indirizzo Milano
    if not validate_milan_address(indirizzo_pulito):
        quality_stats["indirizzi_non_milano"] += 1
        quality_stats["ristoranti_scartati"] += 1
        quality_stats["motivi_scarto"]["indirizzo_non_milano"] += 1
        logger.warning(f"Ristorante '{nome_ristorante}' scartato: indirizzo non Milano")
        continue
    
    # Controllo duplicati
    restaurant_hash = generate_restaurant_hash(nome_ristorante, indirizzo_pulito)
    if restaurant_hash in seen_restaurants:
        quality_stats["ristoranti_duplicati"] += 1
        quality_stats["ristoranti_scartati"] += 1
        quality_stats["motivi_scarto"]["duplicato"] += 1
        logger.warning(f"Ristorante duplicato trovato: '{nome_ristorante}'")
        continue
    else:
        seen_restaurants[restaurant_hash] = {
            "nome": nome_ristorante,
            "indirizzo": indirizzo_pulito
        }
    
    # Processa piatti
    menu_pulito = []
    piatti_validi = 0
    
    for p in r.get("piatti", []):
        quality_stats["piatti_processati"] += 1
        
        nome_piatto = p.get("nome", "").strip()
        
        # Valida nome piatto
        nome_valido, motivo = validate_dish_name(nome_piatto)
        if not nome_valido:
            quality_stats["piatti_scartati"] += 1
            quality_stats["motivi_scarto"][f"piatto_{motivo.lower().replace(' ', '_')}"] += 1
            continue
        
        # Valida prezzo
        prezzo, prezzo_valido = validate_price(p.get("prezzo", ""))
        if not prezzo_valido and prezzo > 0:
            quality_stats["piatti_prezzo_anomalo"] += 1
            logger.warning(f"Prezzo anomalo per piatto '{nome_piatto}': €{prezzo}")
        
        # Pulisci ingredienti
        ingredienti = clean_ingredients(p.get("ingredienti", ""))
        
        # Salta piatti senza ingredienti validi
        if not has_valid_ingredients(ingredienti):
            quality_stats["piatti_scartati"] += 1
            quality_stats["motivi_scarto"]["ingredienti_non_validi"] += 1
            continue
        
        # Classifica tipo piatto
        tipo_piatto = classify_dish_type(nome_piatto, nome_ristorante, ingredienti)
        tipo_piatto_stats[tipo_piatto] += 1
        
        # Crea piatto pulito
        piatto_id = ObjectId()
        
        piatto = {
            "_id": piatto_id,
            "nome_ristorante": nome_ristorante,
            "nome": nome_piatto,
            "tipo_piatto": tipo_piatto,
            "ingredienti": ingredienti,
            "prezzo": prezzo,
            "fonte": "Glovo",
            "data_elaborazione": datetime.now().isoformat()
        }
        
        menu_pulito.append(piatto)
        piatti_validi += 1
    
    # Controlla se il ristorante ha abbastanza piatti
    if piatti_validi < DATA_QUALITY["min_dishes_per_restaurant"]:
        quality_stats["ristoranti_scartati"] += 1
        quality_stats["motivi_scarto"]["troppi_pochi_piatti"] += 1
        logger.warning(f"Ristorante '{nome_ristorante}' scartato: solo {piatti_validi} piatti validi")
        continue
    
    # Crea ristorante pulito
    ristorante_id = ObjectId()
    
    # Aggiorna restaurant_id nei piatti 
    for piatto in menu_pulito:
    #    piatto["ristorante_id"] = ristorante_id
        piatti_clean.append(piatto)
    
    # Valida orari
    orari_validati = validate_opening_hours(r.get("orari", []))
    
    r_clean = {
        "_id": ristorante_id,
        "nome": nome_ristorante,
        "tipo_cucina": r.get("tipo", "").strip(),
        "indirizzo": indirizzo_pulito,
        "telefono": r.get("telefono", "").strip(),
        "orari": orari_validati,
        "numero_piatti": len(menu_pulito),
        "menu": menu_pulito,
        "fonte": "Glovo",
        "data_elaborazione": datetime.now().isoformat()
    }
    
    ristoranti_clean.append(r_clean)
    seen_restaurants[restaurant_hash] = r_clean
    
    if (i + 1) % 100 == 0:
        logger.info(f"Processati {i + 1}/{len(raw_data)} ristoranti")
        

# Rimozione duplicati nei piatti
logger.info("Inizio deduplicazione piatti")
print("🧹 Rimozione duplicati piatti...")

initial = len(piatti_clean)
unique_keys = set()
deduped = []

for p in piatti_clean:
    key = f"{p.get('nome', '').strip().lower()}|{p.get('nome_ristorante', '').strip().lower()}"
    if key not in unique_keys:
        unique_keys.add(key)
        deduped.append(p)

removed = initial - len(deduped)
quality_stats["piatti_duplicati_rimossi"] = removed
logger.info(f"Rimossi {removed} duplicati su {initial} piatti")
print(f"✅ Rimossi {removed} duplicati su {initial} piatti")

piatti_clean = deduped

# === 5. SALVATAGGIO FILES ===
logger.info("Salvataggio files risultanti")

# Piatti
output_piatti = os.path.join(os.path.dirname(__file__), "..", "..", ETL_CONFIG["file_paths"]["clean_dishes"])
with open(output_piatti, "w", encoding="utf-8") as f:
    json.dump(
        [{k: convert_objectid(v) for k, v in piatto.items()} for piatto in piatti_clean],
        f, indent=2, ensure_ascii=False
    )

# Ristoranti con menu
output_ristoranti = os.path.join(os.path.dirname(__file__), "..", "..", ETL_CONFIG["file_paths"]["clean_restaurants"])
with open(output_ristoranti, "w", encoding="utf-8") as f:
    json.dump(
        [{**{k: convert_objectid(v) if k != "menu" else v for k, v in r.items()},
          "menu": [{k: convert_objectid(v) for k, v in p.items()} for p in r["menu"]]}
         for r in ristoranti_clean],
        f, indent=2, ensure_ascii=False
    )

# Report qualità dati
quality_report = {
    "timestamp": datetime.now().isoformat(),
    "versione_etl": VERSIONE_ETL,
    "configurazione": ETL_CONFIG,
    "statistiche": dict(quality_stats),
    "distribuzione_tipi_piatto": dict(tipo_piatto_stats),
    "riepilogo": {
        "ristoranti_validi": len(ristoranti_clean),
        "piatti_validi": len(piatti_clean),
        "tasso_successo_ristoranti": len(ristoranti_clean) / quality_stats["ristoranti_processati"] * 100,
        "tasso_successo_piatti": len(piatti_clean) / quality_stats["piatti_processati"] * 100 if quality_stats["piatti_processati"] > 0 else 0
    }
}

quality_report_path = os.path.join(os.path.dirname(__file__), "..", "..", "reports", "quality", "quality_report_glovo.json")
os.makedirs(os.path.dirname(quality_report_path), exist_ok=True)
with open(quality_report_path, "w", encoding="utf-8") as f:
    json.dump(quality_report, f, indent=2, ensure_ascii=False)

# === 6. STATISTICHE FINALI ===
print("\n" + "="*60)
print("REPORT FINALE")
print("="*60)

print(f"\nRISTORANTI:")
print(f"  • Processati: {quality_stats['ristoranti_processati']}")
print(f"  • Validi salvati: {len(ristoranti_clean)}")
print(f"  • Scartati: {quality_stats['ristoranti_scartati']}")
print(f"  • Duplicati trovati: {quality_stats['ristoranti_duplicati']}")
print(f"  • Tasso successo: {len(ristoranti_clean) / quality_stats['ristoranti_processati'] * 100:.1f}%")

print(f"\nPIATTI:")
print(f"  • Processati: {quality_stats['piatti_processati']}")
print(f"  • Validi salvati: {len(piatti_clean)}")
print(f"  • Scartati: {quality_stats['piatti_scartati']}")
print(f"  • Con prezzo anomalo: {quality_stats['piatti_prezzo_anomalo']}")
print(f"  • Tasso successo: {len(piatti_clean) / quality_stats['piatti_processati'] * 100:.1f}%")
print(f"  • Piatti duplicati rimossi: {quality_stats['piatti_duplicati_rimossi']}")

print(f"\n⚠️  PROBLEMI QUALITA':")
print(f"  • Indirizzi non Milano: {quality_stats['indirizzi_non_milano']}")
for motivo, count in quality_stats["motivi_scarto"].items():
    print(f"  • {motivo.replace('_', ' ').title()}: {count}")

# Statistiche avanzate con pandas
if len(piatti_clean) > 0:
    df_piatti = pd.DataFrame(piatti_clean)
    df_ristoranti = pd.DataFrame(ristoranti_clean)
    
    print(f"\nSTATISTICHE:")
    print(f"  • Media piatti per ristorante: {len(piatti_clean) / len(ristoranti_clean):.1f}")
    print(f"  • Prezzo medio: €{df_piatti['prezzo'].mean():.2f}")
    print(f"  • Prezzo mediano: €{df_piatti['prezzo'].median():.2f}")
    print(f"  • Range prezzi: €{df_piatti['prezzo'].min():.2f} - €{df_piatti['prezzo'].max():.2f}")
    
    print(f"\nTOP 5 TIPI CUCINA:")
    for cucina, count in df_ristoranti['tipo_cucina'].value_counts().head().items():
        print(f"  • {cucina}: {count} ristoranti")
    
    print(f"\nTOP 10 TIPI PIATTO:")
    sorted_types = sorted(tipo_piatto_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    for tipo, count in sorted_types:
        percentage = (count / len(piatti_clean)) * 100
        print(f"  • {tipo.title()}: {count} piatti ({percentage:.1f}%)")
    
    print(f"\nPREZZI MEDI PER TIPO PIATTO:")
    for tipo, count in sorted_types[:5]:
        tipo_df = df_piatti[df_piatti['tipo_piatto'] == tipo]
        if len(tipo_df) > 0:
            prezzo_medio = tipo_df['prezzo'].mean()
            print(f"  • {tipo.title()}: €{prezzo_medio:.2f}")

print(f"\nFILES GENERATI:")
print(f"  • {output_ristoranti} ({len(ristoranti_clean)} ristoranti)")
print(f"  • {output_piatti} ({len(piatti_clean)} piatti)")
print(f"  • {quality_report_path} (report qualità)")
print(f"  • {log_path} (log dettagliato)")

logger.info(f"ETL completato: {len(ristoranti_clean)} ristoranti, {len(piatti_clean)} piatti processati")