# 1_transforming.py
# Arricchimento e normalizzazione allergeni

import os
import re
import json
import yaml
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from rapidfuzz import fuzz

# ========= CONFIG =========
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# ========= LOGGING =========
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "1_transforming_allergens.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========= COSTANTI / PARAMETRI =========

# Soglie fuzzy
FUZZY_THRESHOLD = 86  # soglia per match robusti su parole singole
PARTIAL_THRESHOLD = 92

# Parole/etichette da escludere (menu/box/bundle ecc.)
PAROLE_ESCLUSIONE = {
    "menu", "menù", "box", "family box", "combo", "degustazione",
    "bevanda", "bibita", "drink", "coperto", "pane e coperto"
}

# Mappa allergeni
ALLERGENI_MAP: Dict[str, List[str]] = {
    "glutine": [
        "glutine", "farina", "pane", "pasta", "pizza", "spaghetti", "lasagne",
        "gnocchi", "couscous", "orzo", "farro", "grano", "segale", "hamburger",
        "panino", "focaccia", "bruschetta", "piadina", "tigella", "crackers",
        "pastella", "impanatura", "cotoletta" "base pizza", "pasta sfoglia", "pasta frolla", "cracker", "cheesecake", "tiramisu'"
    ],
    "latte": [
        "latte", "formaggio", "parmigiano", "grana", "mozzarella", "gorgonzola",
        "ricotta", "panna", "burro", "besciamella", "yogurt", "stracciatella",
        "provola", "caciocavallo"
    ],
    "uova": ["uova", "uovo", "maionese", "crema pasticcera", "carbonara", "cotoletta", "cheesecake", "tiramisu'"],
    "frutta a guscio": [
        "nocciole", "noci", "mandorle", "pistacchi", "anacardi", "noci pecan",
        "noci brasiliane", "noci macadamia", "crema di nocciole", "nutella", "gianduia"
    ],
    "arachidi": ["arachidi", "burro di arachidi", "peanut"],
    "pesce": [
        "pesce", "acciughe", "alici", "tonno", "salmone", "merluzzo",
        "baccalà", "sgombro", "orata", "branzino"
    ],
    "crostacei": ["gambero", "gamberi", "aragosta", "scampo", "granchio", "astice", "mazzancolla"],
    "molluschi": ["polpo", "calamaro", "cozze", "vongole", "seppie", "totano"],
    "soia": ["soia", "salsa di soia", "tofu", "edamame"],
    "sedano": ["sedano"],
    "lupini": ["lupini"],
    "senape": ["senape", "mostarda", "salse"],
    "sesamo": ["sesamo", "semi di sesamo", "tahini", "hummus"],
    "solfiti": ["solfiti", "vino", "aceto di vino", "anidride solforosa"]
}

# ========= UTILS =========

def _normalize_token(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[’'`]", "'", s)
    return s

def _tokenize_ingredient_list(ingredients_field: Any) -> List[str]:
    """
    Accetta lista o stringa con separatore virgola; restituisce lista normalizzata.
    """
    if ingredients_field is None:
        return []
    if isinstance(ingredients_field, list):
        raw = ingredients_field
    else:
        raw = str(ingredients_field).split(",")
    return [ _normalize_token(x) for x in raw if str(x).strip() ]

def _fuzzy_contains(text: str, keywords: List[str]) -> Tuple[bool, str, int]:
    text = _normalize_token(text)
    for kw in keywords:
        keyword = _normalize_token(kw)

        # 1. match esatto/parole intere
        if keyword in text and len(keyword) > 2:
            return True, keyword, 100

        # 2. parola per parola
        for word in text.split():
            score = fuzz.ratio(keyword, word)
            if score >= FUZZY_THRESHOLD:
                return True, keyword, score

        # 3. partial ratio (più severo)
        partial_score = fuzz.partial_ratio(keyword, text)
        if partial_score >= PARTIAL_THRESHOLD:
            return True, keyword, partial_score

    return False, "", 0

def _is_menu_or_box(nome: str, ingredienti: List[str]) -> bool:
    # nel nome
    found, matched, score = _fuzzy_contains(nome or "", list(PAROLE_ESCLUSIONE))
    if found:
        logger.debug(f"Menu/Box nel nome: '{nome}' -> '{matched}' ({score})")
        return True
    # negli ingredienti
    for ing in ingredienti:
        found, matched, score = _fuzzy_contains(ing, list(PAROLE_ESCLUSIONE)) if False else _fuzzy_contains(ing, list(PAROLE_ESCLUSIONE))  # safeguard if variable renamed altrove
        found, matched, score = _fuzzy_contains(ing, list(PAROLE_ESCLUSIONE))
        if found:
            logger.debug(f"Menu/Box negli ingredienti: '{ing}' -> '{matched}' ({score})")
            return True
    return False

def estrai_allergeni(ingredienti: List[str], tipo_piatto: str, nome_piatto: str = "") -> List[str]:
    """
    Aggiornamento allergeni
    """
    allergeni_trovati = set()

    ingredienti_lower = [i for i in _tokenize_ingredient_list(ingredienti)]
    nome_lower = _normalize_token(nome_piatto or "")
    testi = ingredienti_lower + ([nome_lower] if nome_lower else [])

    # Regole rapide per alcune famiglie
    if (tipo_piatto or "").lower() in {"pizza", "pasta", "hamburger", "kebab", "kebap", "piadina"}:
        allergeni_trovati.add("glutine")

    # Scan dictionary
    for allergene, keywords in ALLERGENI_MAP.items():
        for kw in keywords:
            # ingredienti
            for token in ingredienti_lower:
                exact = kw in token
                fuzzy_ok = fuzz.ratio(kw, token) >= FUZZY_THRESHOLD or fuzz.partial_ratio(kw, token) >= PARTIAL_THRESHOLD
                if exact or fuzzy_ok:
                    allergeni_trovati.add(allergene)
                    break
            if allergene in allergeni_trovati:
                break
        # Nome del piatto
        if allergene not in allergeni_trovati and nome_lower:
            exact = any(kw in nome_lower for kw in keywords)
            fuzzy_ok = any(fuzz.ratio(kw, nome_lower) >= FUZZY_THRESHOLD for kw in keywords)
            if exact or fuzzy_ok:
                allergeni_trovati.add(allergene)

    return sorted(allergeni_trovati)

# ========= PIPELINE =========

def carica_input() -> List[Dict[str, Any]]:
    """
    Carica i dataset dai path indicati in config. Ci si aspetta un JSON di piatti.
    """
    input_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["clean_dishes"])
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Caricati {len(data)} piatti da {input_file}")
    return data

def filtra_piatto(p: Dict[str, Any]) -> bool:
    """
    Esegue le stesse esclusioni del vecchio script (p.es. bibite/altro/menu/box).
    """
    tipo = _normalize_token(p.get("tipo_piatto", ""))
    nome = p.get("nome", "") or p.get("name", "")
    ingredienti = _tokenize_ingredient_list(p.get("ingredienti"))
    if tipo in {"altro", "bibite", "bevande"}:
        return False
    if _is_menu_or_box(nome, ingredienti):
        return False
    return True

def arricchisci_allergeni(piatti: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    scartati = 0
    for p in piatti:
        if not filtra_piatto(p):
            scartati += 1
            continue
        enriched = dict(p)
        allergens = estrai_allergeni(
            ingredienti=_tokenize_ingredient_list(p.get("ingredienti")),
            tipo_piatto=p.get("tipo_piatto", ""),
            nome_piatto=p.get("nome", "") or p.get("name", "")
        )
        enriched["allergeni"] = allergens
        out.append(enriched)
    logger.info(f"Piatti tenuti: {len(out)} | Scartati (bibite/altro/menu/box): {scartati}")
    return out

def salva_output(piatti: List[Dict[str, Any]]) -> None:
    output_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["dishes_with_allergens"])
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(piatti, f, ensure_ascii=False, indent=2)
    logger.info(f"Salvati {len(piatti)} piatti con allergeni in {output_file}")

def main():
    logger.info("=== INIZIO 1_transforming (allergeni) ===")
    data = carica_input()
    enriched = arricchisci_allergeni(data)
    salva_output(enriched)
    logger.info("=== FINE 1_transforming ===")

if __name__ == "__main__":
    main()
