# Stima Calorie e Allergeni per Piatti Milano - VERSIONE OTTIMIZZATA

import json
import yaml
import os
import re
from rapidfuzz import fuzz
import requests
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# === CONFIG ===
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# === LOGGING ===
log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "etl", "stima_calorie_allergeni_llama.log")
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

# === OLLAMA ===
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"
MAX_WORKERS = 3  # Threading per parallelizzare
TIMEOUT = 15     # Timeout per evitare blocchi
MAX_RETRIES = 2  # Numero massimo di retry

# === PARAMETRI TEST ===
TEST_MODE = False   # Cambia a False per processare tutti i piatti
TEST_LIMIT = 20   # Numero di piatti da testare

# === FILE CACHE ===
CACHE_FILE = "piatti_unici_cache.json"

# === FILTRI ESCLUSIONI ===
PAROLE_ESCLUSIONE = [
    "menu", "menù", "Menù","box", "crea", "componi", "scegli", "personalizza",
    "combo", "offerta", "promo", "formula", "kit", "set", "pacchetto"
]

# === Dizionario Allergenico ===
ALLERGENI_DIZ = {
    "glutine": [
        # Prodotti da forno
        "farina", "pane", "pizza", "pasta", "piadina", "impasto", "frumento", "orzo", "avena", "segale",
        # Tipi specifici
        "hamburger", "panino", "focaccia", "bruschetta", "crostino", "toast", "sandwich",
        "gnocchi", "lasagne", "tortellini", "ravioli", "tagliatelle", "spaghetti",
        # Impasti e basi
        "base pizza", "pasta sfoglia", "pasta frolla", "cracker"
    ],
    "latte": ["mozzarella", "formaggio", "burro", "latte", "parmigiano", "gorgonzola", "ricotta", "mascarpone", "stracchino", "fontina", "pecorino"],
    "uova": ["uovo", "uova", "frittata", "carbonara", "maionese"],
    "pesce": ["tonno", "salmone", "merluzzo", "acciughe", "pesce", "baccalà", "branzino", "orata", "spigola"],
    "soia": ["soia", "salsa di soia", "tofu", "edamame"],
    "frutta a guscio": ["noci", "nocciole", "mandorle", "anacardi", "pistacchi", "pinoli", "noci pecan"],
    "arachidi": ["arachidi", "burro di arachidi"],
    "sedano": ["sedano", "sedano rapa"],
    "senape": ["senape", "mostarda"],
    "sesamo": ["sesamo", "semi di sesamo", "tahini", "hummus"],
    "molluschi": ["polpo", "calamaro", "cozze", "vongole", "seppie", "totano"],
    "crostacei": ["gambero", "aragosta", "scampo", "granchio", "astice", "mazzancolla"]
}

# === Funzioni di Filtro con Fuzzy Matching ===
def is_menu_or_box_item(piatto):
    """Controlla se il piatto è un menu/box/offerta da escludere usando fuzzy matching"""
    nome = piatto.get('nome', '').lower()
    ingredienti = [ing.lower() for ing in piatto.get('ingredienti', [])]
    
    # Soglia fuzzy per parole di esclusione (più permissiva per sicurezza)
    FUZZY_THRESHOLD = 88
    
    def fuzzy_contains(text, keywords):
        """Controlla se il testo contiene keywords con fuzzy matching"""
        for keyword in keywords:
            # 1. Exact match (più veloce)
            if keyword in text:
                return True, keyword, 100
            
            # 2. Fuzzy match per variazioni
            # Controlla ogni parola del testo separatamente
            words = text.split()
            for word in words:
                score = fuzz.ratio(keyword, word)
                if score >= FUZZY_THRESHOLD:
                    return True, keyword, score
            
            # 3. Partial ratio per match parziali
            partial_score = fuzz.partial_ratio(keyword, text)
            if partial_score >= 92:  # Soglia più alta per partial
                return True, keyword, partial_score
        
        return False, None, 0
    
    # Controlla nel nome
    found, matched_word, score = fuzzy_contains(nome, PAROLE_ESCLUSIONE)
    if found:
        logger.debug(f"Menu/Box rilevato nel nome: '{nome}' -> '{matched_word}' (score: {score})")
        return True
    
    # Controlla negli ingredienti
    for ingrediente in ingredienti:
        found, matched_word, score = fuzzy_contains(ingrediente, PAROLE_ESCLUSIONE)
        if found:
            logger.debug(f"Menu/Box rilevato in ingrediente: '{ingrediente}' -> '{matched_word}' (score: {score})")
            return True
    
    return False

# === Funzioni di Normalizzazione ===
def normalize_name(name):
    name = re.sub(r'\b(pizza|piadina|hamburger|panino|piatto di)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-zA-Z0-9 ]', '', name)
    return name.strip().lower()

def simplify_ingredient(ingredient):
    ingredient = re.sub(r'\b(fresco|italiano|stagionato|piccante|di qualità|speciale)\b', '', ingredient, flags=re.IGNORECASE)
    return ingredient.strip().lower()

# === Prompt con Vincoli Calorici ===
def create_optimized_prompt(piatto):
    ingredienti_principali = piatto['ingredienti'][:5]  # Prendi solo i primi 5 ingredienti
    ingredienti_str = ', '.join(ingredienti_principali)
    
    # Definisci range calorici realistici per tipo piatto
    calorie_ranges = {
        "pizza": "700-1200",
        "pasta": "400-800", 
        "hamburger": "500-900",
        "panino": "300-600",
        "insalata": "100-400",
        "antipasto": "150-500",
        "primo": "400-700",
        "secondo": "300-800",
        "dolce": "250-600",
        "contorno": "50-300"
    }
    
    range_calorico = calorie_ranges.get(piatto['tipo_piatto'], "300-800")
    
    prompt = f"""Analizza questo piatto italiano: {piatto['tipo_piatto']} - {ingredienti_str}

IMPORTANTE: Per {piatto['tipo_piatto']}, le calorie devono essere nel range {range_calorico}.

Rispondi SOLO con JSON valido, senza spiegazioni:
{{"calorie": [numero_intero], "healthy": [true/false]}}

Esempi realistici:
- Pizza margherita: {{"calorie": 750, "healthy": false}}
- Pizza con patate: {{"calorie": 850, "healthy": false}} 
- Hamburger con patatine: {{"calorie": 750, "healthy": false}}
- Pasta al pomodoro: {{"calorie": 300, "healthy": true}}
- Insalata mista: {{"calorie": 100, "healthy": true}}

Risposta:"""
    
    return prompt

# === Parser JSON Robusto ===
def parse_llama_response(response):
    if not response:
        return None
    
    try:
        # Rimuovi eventuali markdown e pulisci
        response = re.sub(r'```json|```', '', response).strip()
        
        # Cerca il primo pattern JSON valido
        json_match = re.search(r'\{[^{}]*"calorie"[^{}]*"healthy"[^{}]*\}', response)
        if json_match:
            json_str = json_match.group(0)
            parsed = json.loads(json_str)
            
            # Normalizza il campo healthy come boolean
            if 'healthy' in parsed:
                healthy_val = parsed['healthy']
                
                # Se è già un boolean, mantienilo
                if isinstance(healthy_val, bool):
                    parsed['healthy'] = healthy_val
                else:
                    # Se è una stringa, convertila
                    healthy_str = str(healthy_val).lower()
                    if healthy_str in ['true', 'healthy', '1', 'yes', 'sano']:
                        parsed['healthy'] = True
                    else:
                        parsed['healthy'] = False
            
            # Assicurati che calorie sia un intero
            if 'calorie' in parsed:
                try:
                    parsed['calorie'] = int(parsed['calorie'])
                except:
                    parsed['calorie'] = None
            
            return parsed
            
    except Exception as e:
        logger.warning(f"Errore parsing risposta: {e}, risposta: {response[:100]}")
    
    return None

# === Funzione Chiamata Ollama Ottimizzata ===
def llama_infer_optimized(prompt):
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,      
            "top_p": 0.9,
            "num_predict": 80,       
            "stop": ["\n\n", "Spiegazione:", "In sintesi", "Tuttavia", "Inoltre"]
        }
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(OLLAMA_API_URL, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            result = response.json()['response'].strip()
            return result
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout tentativo {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
        except Exception as e:
            logger.warning(f"Errore chiamata Ollama tentativo {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
    
    return None

# === Funzione Processing Singolo Piatto ===
def process_single_piatto(piatto_data):
    idx, piatto = piatto_data
    
    try:
        prompt = create_optimized_prompt(piatto)
        response = llama_infer_optimized(prompt)
        parsed = parse_llama_response(response)
        
        if parsed and 'calorie' in parsed and 'healthy' in parsed:
            return (piatto['nome'], tuple(piatto['ingredienti']), parsed)
        else:
            logger.warning(f"Parsing fallito per piatto {idx}: {piatto['nome']}")
            return (piatto['nome'], tuple(piatto['ingredienti']), None)
            
    except Exception as e:
        logger.error(f"Errore processing piatto {idx}: {e}")
        return (piatto['nome'], tuple(piatto['ingredienti']), None)

# === Cache Management ===
def save_piatti_unici_cache(piatti_unici):
    cache_path = os.path.join(os.path.dirname(__file__), CACHE_FILE)
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(piatti_unici, f, ensure_ascii=False, indent=2)
        logger.info(f"Cache piatti unici salvata: {cache_path}")
    except Exception as e:
        logger.warning(f"Errore salvataggio cache: {e}")

def load_piatti_unici_cache():
    cache_path = os.path.join(os.path.dirname(__file__), CACHE_FILE)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                piatti_unici = json.load(f)
            logger.info(f"Cache piatti unici caricata: {len(piatti_unici)} piatti")
            return piatti_unici
        except Exception as e:
            logger.warning(f"Errore caricamento cache: {e}")
    return None

def get_cache_stats():
    cache_path = os.path.join(os.path.dirname(__file__), CACHE_FILE)
    if os.path.exists(cache_path):
        mod_time = os.path.getmtime(cache_path)
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mod_time))
    return None

def stima_calorie_batch(piatti_unici):
    logger.info(f"Inizio stima calorie per {len(piatti_unici)} piatti con {MAX_WORKERS} thread")
    risultati_stima = {}
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Prepara i task
        piatto_tasks = [(idx, piatto) for idx, piatto in enumerate(piatti_unici)]
        
        # Sottometti tutti i task
        future_to_piatto = {
            executor.submit(process_single_piatto, task): task[0] 
            for task in piatto_tasks
        }
        
        # Processa i risultati man mano che arrivano
        completed = 0
        successful = 0
        
        for future in as_completed(future_to_piatto):
            nome, ingredienti_tuple, result = future.result()
            
            if result:
                risultati_stima[(nome, ingredienti_tuple)] = result
                successful += 1
            
            completed += 1
            
            if completed % 10 == 0:
                elapsed = time.time() - start_time
                avg_time = elapsed / completed
                remaining = len(piatti_unici) - completed
                eta = remaining * avg_time
                
                logger.info(f"Completati {completed}/{len(piatti_unici)} piatti "
                           f"({successful} successi) - ETA: {eta/60:.1f}min")
    
    total_time = time.time() - start_time
    logger.info(f"Stima completata in {total_time/60:.1f} minuti. "
               f"Successi: {successful}/{len(piatti_unici)} ({successful/len(piatti_unici)*100:.1f}%)")
    
    return risultati_stima

# === Estrazione allergeni con Fuzzy Matching ===
def estrai_allergeni(ingredienti, tipo_piatto):
    allergeni_trovati = set()
    ingredienti_lower = [ing.lower().strip() for ing in ingredienti]
    
    # Regole automatiche per tipo piatto
    if tipo_piatto in ["pizza", "pasta", "hamburger", "kebab", "piadina"]:
        allergeni_trovati.add("glutine")
    
    # Ricerca fuzzy negli ingredienti
    for ingrediente in ingredienti_lower:
        if not ingrediente:  # Skip ingredienti vuoti
            continue
            
        for allergene, keywords in ALLERGENI_DIZ.items():
            if allergene in allergeni_trovati:
                continue  # Già trovato, skip
                
            for keyword in keywords:
                # 1. Exact match (più veloce)
                if keyword in ingrediente:
                    allergeni_trovati.add(allergene)
                    break
                
                # 2. Fuzzy matching per variazioni
                # partial_ratio per match parziali (es. "parmigiano reggiano" vs "parmigiano")
                partial_score = fuzz.partial_ratio(keyword, ingrediente)
                # ratio per match completi con piccole variazioni (es. "mozzarela" vs "mozzarella")
                full_score = fuzz.ratio(keyword, ingrediente)
                
                # Soglie diverse per diversi tipi di match
                if partial_score >= 90 or full_score >= 85:
                    allergeni_trovati.add(allergene)
                    logger.debug(f"Fuzzy match: '{keyword}' -> '{ingrediente}' (partial:{partial_score}, full:{full_score}) -> {allergene}")
                    break
            
            if allergene in allergeni_trovati:
                break  # Esci dal loop keywords se allergene già trovato
    
    return sorted(list(allergeni_trovati))

# === Funzione per testare matching allergeni (per debug) ===
def test_allergeni_matching(ingrediente_test):
    """Funzione di debug per testare il matching degli allergeni"""
    print(f"\nTesting ingredient: '{ingrediente_test}'")
    matches = []
    
    for allergene, keywords in ALLERGENI_DIZ.items():
        for keyword in keywords:
            partial_score = fuzz.partial_ratio(keyword, ingrediente_test.lower())
            full_score = fuzz.ratio(keyword, ingrediente_test.lower())
            
            if partial_score >= 90 or full_score >= 85:
                matches.append((allergene, keyword, partial_score, full_score))
    
    if matches:
        print("Matches found:")
        for allergene, keyword, partial, full in matches:
            print(f"  {allergene}: '{keyword}' (partial:{partial}, full:{full})")
    else:
        print("No matches found")
    
    return matches

# === MAIN EXECUTION ===
def main():
    start_time = time.time()
    logger.info("=== INIZIO ETL STIMA CALORIE E ALLERGENI ===")
    
    # === Carica JSON ===
    try:
        piatti_file = os.path.join(os.path.dirname(__file__), "..", "..", CONFIG["file_paths"]["clean_dishes"])
        with open(piatti_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        logger.info(f"Caricati {len(raw_data)} piatti dal file: {piatti_file}")
    except FileNotFoundError:
        logger.error(f"File '{piatti_file}' non trovato")
        exit(1)
    except Exception as e:
        logger.error(f"Errore caricamento file: {e}")
        exit(1)
    
    # === Filtra tipo_piatto 'altro' ===
    piatti_filtered_altro = [p for p in raw_data if p.get('tipo_piatto') != 'altro']
    logger.info(f"Piatti dopo filtro 'altro': {len(piatti_filtered_altro)}")
    
    # === Filtra menu/box/offerte ===
    piatti_filtered_menu = []
    piatti_esclusi_menu = []
    
    for piatto in piatti_filtered_altro:
        if is_menu_or_box_item(piatto):
            piatti_esclusi_menu.append(piatto)
        else:
            piatti_filtered_menu.append(piatto)
    
    logger.info(f"Piatti dopo filtro menu/box/offerte: {len(piatti_filtered_menu)}")
    logger.info(f"Piatti esclusi per menu/box/offerte: {len(piatti_esclusi_menu)}")
    
    # Esempi di piatti esclusi (per debug) 
    if piatti_esclusi_menu:
        logger.info("Esempi piatti esclusi (con fuzzy matching):")
        for piatto in piatti_esclusi_menu[:5]:
            logger.info(f"  - {piatto['nome']} (ingredienti: {', '.join(piatto['ingredienti'][:3])}...)")
    
    # Usa i piatti filtrati per la deduplicazione
    piatti = piatti_filtered_menu
    
    # === Deduplicazione Piatti (Fuzzy Matching) con Cache AGGIORNATA ===
    # IMPORTANTE: La cache ora viene creata DOPO tutti i filtri
    cache_stats = get_cache_stats()
    if cache_stats:
        logger.info(f"Cache esistente trovata (creata: {cache_stats})")
        use_cache = input("Vuoi usare la cache esistente? (y=usa/N=ricrea): ").lower().startswith('y')
        if use_cache:
            piatti_unici = load_piatti_unici_cache()
            if piatti_unici:
                # Filtra anche la cache per sicurezza
                piatti_unici_filtrati = [p for p in piatti_unici if not is_menu_or_box_item(p)]
                if len(piatti_unici_filtrati) != len(piatti_unici):
                    logger.warning(f"Rimossi {len(piatti_unici) - len(piatti_unici_filtrati)} menu/box dalla cache")
                piatti_unici = piatti_unici_filtrati
                logger.info(f"Usata cache filtrata: {len(piatti_unici)} piatti unici")
            else:
                logger.warning("Cache non valida, procedo con deduplicazione")
                piatti_unici = None
        else:
            piatti_unici = None
    else:
        piatti_unici = None
    
    if piatti_unici is None:
        logger.info("Inizio deduplicazione piatti...")
        piatti_unici = []
        piatti_keys = []
        
        for p in piatti:
            nome_norm = normalize_name(p['nome'])
            ingredienti_norm = sorted([simplify_ingredient(i) for i in p['ingredienti']])
        
            is_duplicate = False
            for idx, (existing_name, existing_ingredients, existing_tipo) in enumerate(piatti_keys):
                nome_sim = fuzz.token_sort_ratio(nome_norm, existing_name)
                common_ingredients = set(ingredienti_norm) & set(existing_ingredients)
                max_ingredients = max(len(existing_ingredients), len(ingredienti_norm), 1)
                ingredient_overlap = len(common_ingredients) / max_ingredients
        
                if nome_sim > 90 and ingredient_overlap >= 0.8 and p['tipo_piatto'] == existing_tipo:
                    is_duplicate = True
                    break
        
            if not is_duplicate:
                piatti_unici.append(p)
                piatti_keys.append((nome_norm, ingredienti_norm, p['tipo_piatto']))
        
        # Salva cache AGGIORNATA (solo piatti validi dopo tutti i filtri)
        save_piatti_unici_cache(piatti_unici)
        logger.info("Cache aggiornata con nuovi filtri menu/box")
    
    logger.info(f"Piatti unici da stimare (dopo deduplicazione): {len(piatti_unici)}")
    
    # === Modalità TEST ===
    if TEST_MODE:
        piatti_unici = piatti_unici[:TEST_LIMIT]
        logger.info(f"MODALITÀ TEST: Processando solo {len(piatti_unici)} piatti")
    
    # === Stima Calorie e Healthy con LLaMA (Multithreaded) ===
    risultati_stima = stima_calorie_batch(piatti_unici)
    
    # === Arricchisci Piatti Originali (solo quelli non esclusi) ===
    logger.info("Arricchimento piatti originali...")
    arricchiti = 0
    
    for p in piatti:
        key = (p['nome'], tuple(p['ingredienti']))
        stima = risultati_stima.get(key)
        
        if stima:
            p['calorie_stimate'] = stima.get('calorie')
            p['healthy'] = stima.get('healthy')  # Ora è già boolean
            arricchiti += 1
        else:
            p['calorie_stimate'] = None
            p['healthy'] = None
    
        # Estrai allergeni (ora con tipo_piatto)
        allergeni = estrai_allergeni(p['ingredienti'], p['tipo_piatto'])
        p['allergeni'] = allergeni
    
    # Non aggiungiamo i piatti esclusi al risultato finale
    # Il file JSON conterrà solo i piatti validi
    tutti_piatti = piatti
    
    logger.info(f"Piatti arricchiti con stime: {arricchiti}/{len(piatti)}")
    logger.info(f"Totale piatti nel risultato finale: {len(tutti_piatti)}")
    
    # === Salva JSON finale ===
    output_file = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed", "piatti_con_stime_allergeni.json")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tutti_piatti, f, ensure_ascii=False, indent=2)
        logger.info(f"File salvato: {output_file}")
    except Exception as e:
        # Fallback nella directory corrente
        fallback_file = "piatti_con_stime_allergeni.json"
        with open(fallback_file, 'w', encoding='utf-8') as f:
            json.dump(tutti_piatti, f, ensure_ascii=False, indent=2)
        logger.warning(f"Salvato in fallback: {fallback_file} (errore path originale: {e})")
    
    # === Statistiche Finali ===
    total_time = time.time() - start_time
    logger.info("=== STATISTICHE FINALI ===")
    logger.info(f"Tempo totale: {total_time/60:.1f} minuti")
    logger.info(f"Piatti totali nel dataset: {len(raw_data)}")
    logger.info(f"Piatti esclusi per tipo 'altro': {len(raw_data) - len(piatti_filtered_altro)}")
    logger.info(f"Piatti esclusi per menu/box/offerte: {len(piatti_esclusi_menu)}")
    logger.info(f"Piatti processati con stima: {len(piatti)}")
    logger.info(f"Piatti unici stimati: {len(risultati_stima)}")
    logger.info(f"Tempo medio per piatto unico: {total_time/len(risultati_stima):.2f} secondi")
    logger.info(f"Throughput: {len(risultati_stima)*3600/total_time:.0f} piatti/ora")
    
    # Statistiche allergeni
    allergeni_stats = {}
    for p in tutti_piatti:
        for allergene in p.get('allergeni', []):
            allergeni_stats[allergene] = allergeni_stats.get(allergene, 0) + 1
    
    logger.info(f"Allergeni più comuni: {dict(sorted(allergeni_stats.items(), key=lambda x: x[1], reverse=True)[:5])}")
    
    # Statistiche fuzzy matching (se debug attivo)
    fuzzy_matches = sum(1 for p in tutti_piatti if len(p.get('allergeni', [])) > 0)
    logger.info(f"Piatti con almeno un allergene rilevato: {fuzzy_matches}/{len(tutti_piatti)} ({fuzzy_matches/len(tutti_piatti)*100:.1f}%)")
    
    # Statistiche healthy (solo piatti validi)
    healthy_count = sum(1 for p in tutti_piatti if p.get('healthy') is True)
    unhealthy_count = sum(1 for p in tutti_piatti if p.get('healthy') is False)
    no_data_count = sum(1 for p in tutti_piatti if p.get('healthy') is None)
    
    logger.info(f"Piatti healthy: {healthy_count}, unhealthy: {unhealthy_count}, senza dati: {no_data_count}")
    
    logger.info("=== ETL COMPLETATO ===")

if __name__ == "__main__":
    main()