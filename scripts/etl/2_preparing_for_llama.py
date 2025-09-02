# 2_preparing_for_llama.py
# Pulizia, dedup, normalizzazione, costruzione piatti unici per stima con LLaMA

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
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "2_preparing_for_llama.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========= NORMALIZZAZIONI =========

def _normalize_text(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[’`]", "'", s)
    s = re.sub(r"[^a-z0-9àèéìòóùç' ]", "", s)
    return s

def _tokenize_ingredients(ingredients_field) -> List[str]:
    if ingredients_field is None:
        return []
    if isinstance(ingredients_field, list):
        raw = ingredients_field
    else:
        raw = str(ingredients_field).split(",")
    out = []
    for x in raw:
        x = _normalize_text(x)
        if x:
            out.append(x)
    return out

def _dedup_key(nome: str, ingredienti: List[str]) -> Tuple[str, Tuple[str, ...]]:
    return (_normalize_text(nome), tuple(sorted(set(_tokenize_ingredients(ingredienti)))))

def _similar_name(a: str, b: str) -> int:
    return fuzz.ratio(_normalize_text(a), _normalize_text(b))

def _ingredients_overlap(a: List[str], b: List[str]) -> float:
    A, B = set(_tokenize_ingredients(a)), set(_tokenize_ingredients(b))
    if not A or not B:
        return 0.0
    return len(A & B) / max(len(A), len(B))

# ========= PIPELINE =========

def carica_input_con_allergeni() -> List[Dict[str, Any]]:
    input_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["dishes_with_allergens"])
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Caricati {len(data)} piatti (già arricchiti con allergeni) da {input_file}")
    return data

def unifica_piatti(piatti: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Unifica piatti simili (per nome + overlap ingredienti) mantenendo allergeni già calcolati.
    """
    clusters: List[Dict[str, Any]] = []

    for p in piatti:
        nome = p.get("nome") or p.get("name", "")
        ingr = p.get("ingredienti", [])
        matched = False

        for c in clusters:
            score = _similar_name(nome, c["nome"])
            overlap = _ingredients_overlap(ingr, c["ingredienti"])
            if score >= 90 or (score >= 80 and overlap >= 0.5):
                # merge semplice: unione ingredienti e allergeni
                c["ingredienti"] = sorted(set(_tokenize_ingredients(c["ingredienti"]) + _tokenize_ingredients(ingr)))
                c["allergeni"] = sorted(set((c.get("allergeni") or []) + (p.get("allergeni") or [])))
                # conserva qualche campo utile
                for k in ["tipo_piatto", "categoria", "sottocategoria"]:
                    if not c.get(k) and p.get(k):
                        c[k] = p[k]
                matched = True
                break

        if not matched:
            clusters.append({
                "nome": nome,
                "ingredienti": _tokenize_ingredients(ingr),
                "allergeni": sorted(set(p.get("allergeni") or [])),
                "tipo_piatto": p.get("tipo_piatto"),
                "categoria": p.get("categoria"),
                "sottocategoria": p.get("sottocategoria"),
            })

    logger.info(f"Unificati {len(piatti)} -> {len(clusters)} piatti")
    return clusters

def salva_piatti_unici(piatti_unici: List[Dict[str, Any]]) -> None:
    output_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["unique_dishes_for_llama"])
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(piatti_unici, f, ensure_ascii=False, indent=2)
    logger.info(f"Salvati {len(piatti_unici)} piatti unici per LLaMA in {output_file}")

def main():
    logger.info("=== INIZIO 2_preparing_for_llama ===")
    data = carica_input_con_allergeni()
    unici = unifica_piatti(data)
    salva_piatti_unici(unici)
    logger.info("=== FINE 2_preparing_for_llama ===")

if __name__ == "__main__":
    main()
