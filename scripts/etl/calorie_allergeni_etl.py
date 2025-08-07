import json
import yaml
import os
import re
import logging
from pathlib import Path

# Carica configurazione
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# === CONFIGURAZIONE LOGGING ===
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "calorie_allergeni_etl.log")
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

logger.info("Inizio processo")

# === CARICA PIATTI CLEAN ===
try:
    input_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["clean_dishes"])
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    logger.info(f"Caricati {len(raw_data)} piatti dal file: {input_file}")
except FileNotFoundError:
    logger.error(f"File '{input_file}' non trovato")
    exit(1)

output_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["dishes"])

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("processing.log"),
        logging.StreamHandler()
    ]
)

# === Calorie per 100g ===
CALORIE_PER_100G = {
    "mozzarella": 280,
    "patate": 77,
    "pomodoro": 18,
    "olio": 884,
    "olio d'oliva": 884,
    "pesto": 450,
    "prezzemolo": 36,
    "prosciutto": 145,
    "salame": 300,
    "pollo": 165,
    "manzo": 250,
    "tonno": 132,
    "formaggio": 350,
    "zucchine": 17,
    "melanzane": 25,
    "funghi": 22,
    "rucola": 25,
    "cipolla": 40,
    "peperoni": 20,
    "spinaci": 23,
    "carciofi": 47,
    "wurstel": 270,
    "gorgonzola": 330,
    "salsiccia": 300,
    "bresaola": 151,
    "speck": 250,
    "ricotta": 170,
    "acciughe": 210,
    "salmone": 208,
    "basilico": 23,
    "origano": 265,
    "farina": 364,
    "pane": 265,
    "burro": 717,
    "uova": 143,
    "soia": 446,
    "gamberi": 99,
    "granchio": 87,
    "cozze": 172
}

# === Peso medio per tipo piatto (g) ===
PESO_MEDIO_TIPO_PIATTO = {
    "pizza": 300,
    "hamburger": 300,
    "carne": 280,
    "insalata": 250,
    "pesce": 280,
    "pasta": 300,
    "sushi": 200,
    "kebab": 350,
    "gelato": 150,
    "dolce": 180,
    "altro": 300
}

# === Ingredienti considerati non healthy ===
UNHEALTHY_INGREDIENTS = {
    "salame", "wurstel", "salsiccia", "speck", "olio", "olio d'oliva", "formaggio", "gorgonzola",
    "mozzarella", "pesto", "fritto", "maionese", "panato", "burro"
}

# === Dizionario Ingredienti → Allergeni ===
ALLERGENI_MAP = {
    # Latticini
    "mozzarella": ["latte"],
    "formaggio": ["latte"],
    "gorgonzola": ["latte"],
    "burro": ["latte"],
    "ricotta": ["latte"],
    "parmigiano": ["latte"],

    # Pesti
    "pesto genovese": ["frutta a guscio", "latte"],
    "pesto di noci": ["frutta a guscio"],
    "pesto di pistacchio": ["frutta a guscio"],
    "pesto di mandorle": ["frutta a guscio"],
    "pesto di prezzemolo": [],
    "pesto di zucchine": [],

    # Frutta a guscio
    "noci": ["frutta a guscio"],
    "nocciole": ["frutta a guscio"],
    "mandorle": ["frutta a guscio"],
    "arachidi": ["arachidi"],

    # Uova
    "uova": ["uova"],

    # Pesce
    "tonno": ["pesce"],
    "acciughe": ["pesce"],
    "salmone": ["pesce"],

    # Glutine
    "farina": ["glutine"],
    "pane": ["glutine"],
    "pasta": ["glutine"],

    # Soia
    "soia": ["soia"],
    "wurstel": ["soia"],

    # Crostacei/Molluschi
    "gamberi": ["crostacei"],
    "granchio": ["crostacei"],
    "cozze": ["molluschi"],
    "molluschi": ["molluschi"]
}

# === Funzioni ===
def extract_keywords(text):
    tokens = re.split(r'[,\-–\n]| con | e | ed | alla | al | ai | alle | di ', text.lower())
    return [t.strip() for t in tokens if t.strip()]

def stima_calorie(ingredienti, tipo_piatto):
    calorie_totali = 0
    count = 0
    for ingrediente in ingredienti:
        ingrediente_clean = ingrediente.lower().strip()
        if ingrediente_clean in CALORIE_PER_100G:
            calorie_totali += CALORIE_PER_100G[ingrediente_clean]
            count += 1
    if count == 0:
        return None
    media_100g = calorie_totali / count
    peso_piatto = PESO_MEDIO_TIPO_PIATTO.get(tipo_piatto, 300)
    return round((media_100g * peso_piatto) / 100, 1)

def is_healthy(calorie, ingredienti):
    if calorie is None or calorie > 600:
        return False
    for ingr in ingredienti:
        if any(u in ingr.lower() for u in UNHEALTHY_INGREDIENTS):
            return False
    return True

def rileva_allergeni(ingredienti):
    allergeni_trovati = set()
    for ingr in ingredienti:
        ingr_clean = ingr.lower().strip()

        # Match esatto (es. "pesto di prezzemolo")
        if ingr_clean in ALLERGENI_MAP:
            allergeni_trovati.update(ALLERGENI_MAP[ingr_clean])
            continue

        # Match parziale fallback (es. "pesto")
        for chiave, allergeni in ALLERGENI_MAP.items():
            if chiave in ingr_clean:
                allergeni_trovati.update(allergeni)
    return sorted(allergeni_trovati) if allergeni_trovati else []

# === Elaborazione ===
def process_piatti(input_path, output_path):
    if not Path(input_path).exists():
        logging.error(f"File non trovato: {input_path}")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        piatti = json.load(f)

    logging.info(f"Totale piatti da elaborare: {len(piatti)}")
    processed = []
    missing_calorie = 0

    for i, piatto in enumerate(piatti):
        try:
            raw_ingredienti = piatto.get("ingredienti", [])
            tipo_piatto = piatto.get("tipo_piatto", "altro").lower()
            keywords = []
            for item in raw_ingredienti:
                keywords.extend(extract_keywords(item))

            # Aggiunta automatica ingredienti impliciti per pizza
            if tipo_piatto == "pizza":
                if not any("farina" in k for k in keywords):
                    keywords.append("farina")
                if not any("pomodoro" in k for k in keywords):
                    keywords.append("pomodoro")

            cal = stima_calorie(keywords, tipo_piatto)
            healthy = is_healthy(cal, keywords)
            allergeni = rileva_allergeni(keywords)

            if cal is None:
                missing_calorie += 1

            piatto["calorie_stimate"] = cal
            piatto["healthy"] = healthy
            piatto["allergeni"] = allergeni
            processed.append(piatto)

            if (i + 1) % 5000 == 0:
                logging.info(f"Elaborati {i + 1} piatti...")

        except Exception as e:
            logging.exception(f"Errore elaborando piatto ID {piatto.get('_id')}: {e}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2, ensure_ascii=False)

    logging.info(f"✔️ Elaborazione completata. Piatti senza calorie stimate: {missing_calorie}")
    logging.info(f"📁 Output salvato in: {output_path}")

# === Avvio script ===
if __name__ == "__main__":
    process_piatti(input_file, output_file)
