# 3_estimating.py
# Stima calorie con Ollama/LLaMA sui piatti unici, parsing robusto, retry, parallelismo, merge finale

import os
import re
import json
import yaml
import time
import logging
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= CONFIG =========
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# ========= LOGGING =========
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "3_estimating_llama.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========= OLLAMA =========
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"
MAX_WORKERS = 3
TIMEOUT = 15
MAX_RETRIES = 2

TEST_MODE = False   # metti True per test su subset
TEST_LIMIT = 20

# ========= PROMPTING & PARSING =========

PROMPT_TEMPLATE = (
    "Sei un nutrizionista. Stima le calorie totali del piatto seguente e indica se è healthy.\n"
    "Rispondi SOLO con un oggetto JSON come: {\"calorie\": <numero intero>, \"healthy\": <true|false>}.\n"
    "PIATTO: {nome}\n"
    "INGREDIENTI: {ingredienti}\n"
    "ALLERGENI (se noti): {allergeni}\n"
)

def _build_prompt(d: Dict[str, Any]) -> str:
    nome = d.get("nome", "")
    ingreds = ", ".join(d.get("ingredienti") or [])
    allergens = ", ".join(d.get("allergeni") or [])
    return PROMPT_TEMPLATE.format(nome=nome, ingredienti=ingreds, allergeni=allergens)

JSON_OBJ_RE = re.compile(r'\{[^{}]*"calorie"[^{}]*"healthy"[^{}]*\}')

def _parse_llama_output(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    m = JSON_OBJ_RE.search(text)
    if not m:
        # fallback: cifra isolata
        try:
            cals = int(re.findall(r"(\d{2,5})", text)[0])
            return {"calorie": cals, "healthy": None}
        except Exception:
            return None
    try:
        candidate = json.loads(m.group(0))
        # normalizza types
        cals = int(candidate.get("calorie"))
        healthy = candidate.get("healthy")
        if isinstance(healthy, str):
            healthy = healthy.strip().lower() in {"true", "si", "sì", "yes", "y"}
        return {"calorie": cals, "healthy": bool(healthy) if healthy is not None else None}
    except Exception:
        return None

def _call_ollama(prompt: str) -> Optional[Dict[str, Any]]:
    payload = {"model": MODEL, "prompt": prompt, "stream": False}
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.post(OLLAMA_API_URL, json=payload, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            parsed = _parse_llama_output(data.get("response", ""))
            if parsed:
                return parsed
            logger.warning("Parsing fallito, risposta grezza: %s", data.get("response", "")[:200])
        except requests.Timeout:
            logger.warning(f"Timeout tentativo {attempt+1}/{MAX_RETRIES}")
        except Exception as e:
            logger.warning(f"Errore chiamata Ollama tentativo {attempt+1}/{MAX_RETRIES}: {e}")
        time.sleep(0.6 * (attempt + 1))
    return None

# ========= IO =========

def carica_piatti_unici() -> List[Dict[str, Any]]:
    file_unici = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["unique_dishes_for_llama"])
    with open(file_unici, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Piatti unici per stima: {len(data)}")
    if TEST_MODE:
        data = data[:TEST_LIMIT]
        logger.info(f"TEST_MODE attivo: userò solo {len(data)} piatti")
    return data

def salva_stime(stime: List[Dict[str, Any]]) -> None:
    out_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["unique_dishes_estimations"])
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(stime, f, ensure_ascii=False, indent=2)
    logger.info(f"Salvate {len(stime)} stime uniche in {out_file}")

def merge_stime_su_piatti(stime_uniche: List[Dict[str, Any]]) -> None:
    """
    Merge delle stime sui piatti “completi” (con allergeni) per output finale.
    """
    src_allergeni = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["dishes_with_allergens"])
    with open(src_allergeni, "r", encoding="utf-8") as f:
        piatti_allergeni = json.load(f)

    # indicizza per nome normalizzato + set ingredienti
    def key(d):
        return ( (d.get("nome") or "").lower().strip(), tuple(sorted(d.get("ingredienti") or [])) )

    idx = { key(d): d for d in stime_uniche }

    merged = []
    miss = 0
    for p in piatti_allergeni:
        k = key(p)
        base = dict(p)
        if k in idx:
            base.update({
                "calorie_stimate": idx[k].get("calorie"),
                "healthy": idx[k].get("healthy")
            })
        else:
            miss += 1
        merged.append(base)

    out_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["final_dishes_with_estimations"])
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    logger.info(f"Output finale con merge salvato in {out_file} (missing merge: {miss})")

# ========= MAIN =========

def main():
    logger.info("=== INIZIO 3_estimating (Ollama) ===")
    piatti = carica_piatti_unici()

    results = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = []
        for d in piatti:
            prompt = _build_prompt(d)
            futures.append(ex.submit(_call_ollama, prompt))

        for d, fut in zip(piatti, as_completed(futures)):
            parsed = fut.result()
            if parsed:
                results.append({
                    "nome": d.get("nome"),
                    "ingredienti": d.get("ingredienti"),
                    "allergeni": d.get("allergeni"),
                    "calorie": parsed["calorie"],
                    "healthy": parsed.get("healthy")
                })
            else:
                results.append({
                    "nome": d.get("nome"),
                    "ingredienti": d.get("ingredienti"),
                    "allergeni": d.get("allergeni"),
                    "calorie": None,
                    "healthy": None
                })

    elapsed = time.time() - start
    logger.info(f"Stima completata su {len(results)} piatti unici in {elapsed:.1f}s")

    salva_stime(results)
    merge_stime_su_piatti(results)

    logger.info("=== FINE 3_estimating ===")

if __name__ == "__main__":
    from concurrent.futures import as_completed  # import locale per zip(as_completed)
    main()
