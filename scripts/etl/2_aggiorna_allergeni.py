# aggiorna_allergeni.py

import os
import json
import yaml
import logging
from rapidfuzz import fuzz

# === CONFIG ===
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# === LOGGING ===
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "aggiorna_allergeni.log")
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

# === Dizionario Allergenico (AGGIORNATO) ===
ALLERGENI_DIZ = {
    "glutine": [
        "farina", "pane", "pizza", "pasta", "piadina", "impasto", "frumento", "orzo", "avena", "segale",
        "hamburger", "panino", "focaccia", "bruschetta", "crostino", "toast", "sandwich",
        "gnocchi", "lasagne", "tortellini", "ravioli", "tagliatelle", "spaghetti", "cotoletta",
        "base pizza", "pasta sfoglia", "pasta frolla", "cracker", "cheesecake", "tiramisu'"
    ],
    "latte": ["mozzarella", "formaggio", "burro", "latte", "parmigiano", "gorgonzola", "burrata", "ricotta", "mascarpone", "stracchino", "fontina", "pecorino", "yogurt", "philadelphia", "feta", "cheesecake", "tiramisu'", "gelato"],
    "uova": ["uovo", "uova", "frittata", "carbonara", "maionese", "cotoletta", "cheesecake", "tiramisu'"],
    "pesce": ["pesce", "tonno", "salmone", "merluzzo", "acciughe", "pesce", "baccalà", "branzino", "orata", "spigola", "anguilla", "sushi"],
    "soia": ["soia", "salsa di soia", "tofu", "edamame"],
    "frutta a guscio": ["noci", "nocciole", "mandorle", "anacardi", "pistacchi", "pinoli", "noci pecan"],
    "arachidi": ["arachidi", "burro di arachidi"],
    "sedano": ["sedano", "sedano rapa"],
    "senape": ["senape", "mostarda", "salse"],
    "sesamo": ["sesamo", "semi di sesamo", "tahini", "hummus"],
    "molluschi": ["polpo", "calamaro", "cozze", "vongole", "seppie", "totano"],
    "crostacei": ["gambero", "gamberi", "aragosta", "scampo", "granchio", "astice", "mazzancolla"]
}

# === Funzione di estrazione allergeni ===
def estrai_allergeni(ingredienti, tipo_piatto, nome_piatto=""):
    allergeni_trovati = set()

    ingredienti_lower = [ing.lower().strip() for ing in ingredienti if ing]
    nome_lower = nome_piatto.lower().strip() if nome_piatto else ""
    testi_da_analizzare = ingredienti_lower + [nome_lower]

    if tipo_piatto in ["pizza", "pasta", "hamburger", "kebab", "kebap", "piadina"]:
        allergeni_trovati.add("glutine")
    if tipo_piatto in ["pesce", "sushi"]:
        allergeni_trovati.add("pesce")

    for testo in testi_da_analizzare:
        for allergene, keywords in ALLERGENI_DIZ.items():
            if allergene in allergeni_trovati:
                continue
            for keyword in keywords:
                if keyword in testo:
                    allergeni_trovati.add(allergene)
                    break
                partial_score = fuzz.partial_ratio(keyword, testo)
                full_score = fuzz.ratio(keyword, testo)
                if partial_score >= 90 or full_score >= 85:
                    allergeni_trovati.add(allergene)
                    break
            if allergene in allergeni_trovati:
                break

    return sorted(allergeni_trovati)

# === MAIN ===
def main():
    logger.info("=== INIZIO AGGIORNAMENTO ALLERGENI ===")

    input_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["clean_dishes_llama_no_bibite"])
    output_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["update_allergeni_dishes"])

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            piatti = json.load(f)
        logger.info(f"Caricati {len(piatti)} piatti da: {input_file}")
    except Exception as e:
        logger.error(f"Errore caricamento file: {e}")
        return

    aggiornati = 0
    for p in piatti:
        nuovi_allergeni = estrai_allergeni(
            p.get("ingredienti", []),
            p.get("tipo_piatto", ""),
            p.get("nome", "")
        )
        p["allergeni"] = nuovi_allergeni
        aggiornati += 1

    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(piatti, f, ensure_ascii=False, indent=2)
        logger.info(f"Salvati {aggiornati} piatti con allergeni aggiornati in: {output_file}")
    except Exception as e:
        logger.error(f"Errore salvataggio file: {e}")

    logger.info("=== FINE AGGIORNAMENTO ALLERGENI ===")

if __name__ == "__main__":
    main()
