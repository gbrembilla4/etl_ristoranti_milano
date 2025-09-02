"""
Script di pulizia per file JSON di piatti_con_allergeni_aggiornati.json
Analizza valori null e piatti senza allergeni, rimuove piatti 'bibite' anche in base agli ingredienti
"""

import json
import os
import logging
import yaml
from collections import Counter
from rapidfuzz import fuzz

# === CONFIG ===
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# === LOGGING ===
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "analisi_piatti_con_stime.log")
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


def analyze_null_values(data):
    """Analizza piatti con healthy o calorie null"""
    logger.info("=== ANALISI VALORI NULL ===")
    
    # Piatti con calorie null
    calorie_null = [p for p in data if p.get('calorie_stimate') is None]
    logger.info(f"Piatti con calorie null: {len(calorie_null)}")
    
    if calorie_null:
        tipi_calorie_null = Counter(p.get('tipo_piatto', 'N/A') for p in calorie_null)
        logger.info("Distribuzione per tipo piatto (calorie null):")
        for tipo, count in tipi_calorie_null.most_common():
            logger.info(f"  - {tipo}: {count}")
        
        logger.info("\nEsempi piatti con calorie null:")
        for p in calorie_null[:5]:
            logger.info(f"  - {p.get('nome')} ({p.get('tipo_piatto')}) - {p.get('nome_ristorante')}")
    
    # Piatti con healthy null
    healthy_null = [p for p in data if p.get('healthy') is None]
    logger.info(f"\nPiatti con healthy null: {len(healthy_null)}")
    
    if healthy_null:
        tipi_healthy_null = Counter(p.get('tipo_piatto', 'N/A') for p in healthy_null)
        logger.info("Distribuzione per tipo piatto (healthy null):")
        for tipo, count in tipi_healthy_null.most_common():
            logger.info(f"  - {tipo}: {count}")
        
        logger.info("\nEsempi piatti con healthy null:")
        for p in healthy_null[:5]:
            logger.info(f"  - {p.get('nome')} ({p.get('tipo_piatto')}) - {p.get('nome_ristorante')}")


def analyze_no_allergeni(data):
    """Analizza piatti senza allergeni e i loro ingredienti"""
    logger.info("\n=== ANALISI PIATTI SENZA ALLERGENI ===")
    
    no_allergeni = [p for p in data if not p.get('allergeni')]
    logger.info(f"Piatti senza allergeni dichiarati: {len(no_allergeni)}")
    
    if no_allergeni:
        tipi_no_allergeni = Counter(p.get('tipo_piatto', 'N/A') for p in no_allergeni)
        logger.info("Distribuzione per tipo piatto (senza allergeni):")
        for tipo, count in tipi_no_allergeni.most_common():
            logger.info(f"  - {tipo}: {count}")
        
        logger.info("\n=== INGREDIENTI PIÙ COMUNI (piatti senza allergeni) ===")
        all_ingredients = []
        for p in no_allergeni:
            ingredienti = p.get('ingredienti', [])
            all_ingredients.extend([ing.lower().strip() for ing in ingredienti])
        
        ingredient_counter = Counter(all_ingredients)
        logger.info("Top 10 ingredienti più comuni:")
        for ing, count in ingredient_counter.most_common(10):
            logger.info(f"  - '{ing}': {count} volte")
        
        logger.info("\n=== ESEMPI PIATTI SENZA ALLERGENI ===")
        for p in no_allergeni[:5]:
            ingredienti_str = ", ".join(p.get('ingredienti', []))
            logger.info(f"- {p.get('nome')} ({p.get('tipo_piatto')})")
            logger.info(f"  Ristorante: {p.get('nome_ristorante')}")
            logger.info(f"  Ingredienti: {ingredienti_str}")
            logger.info("")


def generate_summary_report(data):
    """Genera un report riassuntivo"""
    logger.info("\n=== REPORT RIASSUNTIVO ===")
    
    total = len(data)
    calorie_null = len([p for p in data if p.get('calorie_stimate') is None])
    healthy_null = len([p for p in data if p.get('healthy') is None])
    no_allergeni = len([p for p in data if not p.get('allergeni')])
    
    logger.info(f"Totale piatti: {total}")
    logger.info(f"Piatti con calorie null: {calorie_null} ({calorie_null/total*100:.1f}%)")
    logger.info(f"Piatti con healthy null: {healthy_null} ({healthy_null/total*100:.1f}%)")
    logger.info(f"Piatti senza allergeni: {no_allergeni} ({no_allergeni/total*100:.1f}%)")


def remove_bibite(data, soglia_fuzzy=85):
    """
    Esclude piatti che sembrano bibite in base a:
    - tipo_piatto == 'bibite' (già filtrati a monte)
    - ingredienti che contengono parole chiave
    - nome del piatto che contiene parole chiave (es. 'Coca-Cola', '%', 'Birra', ecc.)
    """
    keywords = [
        'cl', 'ml', 'coca', 'cola', 'bibita', 'acqua', 'succo', 'cocktail', 'soda',
        'gassata', 'drink', 'bevanda', '%', 'birra', 'vino', 'liquore', 'alcol',
        'spumante', 'amaro', 'digestivo', 'aperitivo', 'spritz', 'mojito', 'bottiglia'
    ]

    def is_probable_bibita(p):
        # Ingredienti
        ingredienti = p.get('ingredienti', [])
        for ing in ingredienti:
            ing_norm = ing.lower().strip()
            for kw in keywords:
                if kw in ing_norm or fuzz.partial_ratio(kw, ing_norm) >= soglia_fuzzy:
                    return True
        
        # Nome piatto
        nome = p.get('nome', '').lower()
        for kw in keywords:
            if kw in nome or fuzz.partial_ratio(kw, nome) >= soglia_fuzzy:
                return True
        
        return False

    filtered = [p for p in data if not is_probable_bibita(p)]
    rimossi = len(data) - len(filtered)
    logger.info(f"Piatti rimossi per nome o ingredienti sospetti di bibite (fuzzy): {rimossi}")
    return filtered


def main():
    logger.info("=== INIZIO SCRIPT PULIZIA PIATTI ===")
    
    # === Carica JSON ===
    try:
        piatti_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["update_allergeni_dishes"])
        with open(piatti_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        logger.info(f"Caricati {len(raw_data)} piatti dal file: {piatti_file}")
    except FileNotFoundError:
        logger.error(f"File '{piatti_file}' non trovato")
        exit(1)
    except Exception as e:
        logger.error(f"Errore caricamento file: {e}")
        exit(1)
    
    # === Filtra tipo_piatto 'bibite' ===
    piatti_filtered = [p for p in raw_data if p.get('tipo_piatto') != 'bibite']
    logger.info(f"Piatti dopo filtro 'bibite': {len(piatti_filtered)} (rimossi {len(raw_data) - len(piatti_filtered)})")
    
    # === Rimuove bibite tramite ingredienti sospetti (fuzzy) ===
    piatti_finali = remove_bibite(piatti_filtered)
    
    # === Analisi ===
    analyze_null_values(piatti_finali)
    analyze_no_allergeni(piatti_finali)
    generate_summary_report(piatti_finali)
    
    logger.info("=== FINE SCRIPT PULIZIA PIATTI ===")


if __name__ == "__main__":
    main()
