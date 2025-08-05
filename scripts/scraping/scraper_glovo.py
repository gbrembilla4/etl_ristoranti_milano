import asyncio
from playwright.async_api import async_playwright
import os
import json
from datetime import datetime
import yaml

# Carica configurazione
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "etl_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

debug_dir = "debug"
os.makedirs(debug_dir, exist_ok=True)

# Zone strategiche per Milano:
tutte_le_zone = [
    "Navigli, Milano",
    "Porta Romana, Milano",
    "Duomo, Milano",
    "Isola, Milano",
    "Brera, Milano",
    "CityLife, Milano",
    "NoLo, Milano",
    "Città Studi, Milano",
    "Bicocca, Milano",
    "Affori, Milano",
    "Lambrate, Milano",
    "Precotto, Milano",
    "Barona, Milano",
    "San Siro, Milano",
    "Quarto Oggiaro, Milano",
    "Ripamonti, Milano"
]

# ZONE PER QUESTA SESSIONE
zones = [
    "Quarto Oggiaro, Milano"
]


# Gestione modale ristorante chiuso
async def gestisci_modale_chiusura(page):
    """Controlla e chiude la modale di chiusura del ristorante se presente"""
    try:
        await asyncio.sleep(1)
        modale_chiusura = page.locator('[data-test-id="similar-stores-modal"]')
        
        # Verifica se la modale è presente
        if await modale_chiusura.count() > 0:
            print("   ⚠️ Modale di chiusura rilevata - chiusura in corso...")
            
            # Metodo 1: Prova a chiudere con il pulsante X
            close_button = page.locator('[data-test-id="base-modal__close"]')
            if await close_button.count() > 0:
                await close_button.click()
                print("   ✅ Modale chiusa con pulsante X")
            else:
                # Metodo 2: Click sull'overlay della modale (basato sull'HTML reale)
                print("   🔄 Tentativo chiusura con click sull'overlay...")
                
                # Cerca l'overlay della modale (elemento che cattura i click per chiudere)
                modal_overlay = page.locator('[data-test-id="modal-overlay"]')
                if await modal_overlay.count() > 0:
                    # Click sull'overlay ma non sul contenuto della modale
                    await modal_overlay.click()
                    print("   ✅ Modale chiusa con click sull'overlay")
                else:
                    # Fallback: cerca per classe CSS
                    modal_overlay_css = page.locator('.modal-overlay')
                    if await modal_overlay_css.count() > 0:
                        await modal_overlay_css.click()
                        print("   ✅ Modale chiusa con click sull'overlay (CSS)")
                    else:
                        print("   ⚠️ Overlay della modale non trovato")
            
            # Aspetta che la modale scompaia
            await asyncio.sleep(1)
            
            # Verifica che sia stata chiusa
            if await modale_chiusura.count() == 0:
                print("   ✅ Modale chiusa con successo")
                return True
            else:
                return False
        
        return False
        
    except Exception as e:
        print(f"   ⚠️ Errore nella gestione modale: {e}")
        # Tentativo di emergenza con click sull'overlay
        try:
            print("   🔄 Tentativo di emergenza...")
            modal_overlay = page.locator('[data-test-id="modal-overlay"]')
            if await modal_overlay.count() > 0:
                await modal_overlay.click()
            await asyncio.sleep(1)
        except:
            pass
        return False

# Estrazione piatti
async def estrai_piatti_ristorante(page):
    """Estrae i piatti dal menu del ristorante"""
    try:
        piatti = []
        
        # Scroll per caricare tutti i piatti
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        
        # Trova tutti i piatti
        product_rows = page.locator('[data-test-id="product-row-content"]')
        
        try:
            await product_rows.first.wait_for(state="visible", timeout=3000)
        except:
            print("   ⚠️ Nessun piatto trovato")
            return []
        
        piatti_count = await product_rows.count()
        print(f"   🍽️ Trovati {piatti_count} piatti")
        
        # Estrai ogni piatto
        for i in range(piatti_count):
            try:
                row = product_rows.nth(i)
                
                # Nome del piatto
                nome_element = row.locator('[data-test-id="product-row-name__highlighter"] span')
                nome = await nome_element.inner_text() if await nome_element.count() > 0 else "Nome non disponibile"
                
                # Ingredienti/Descrizione
                descrizione_element = row.locator('[data-test-id="product-row-description__highlighter"] span')
                ingredienti = await descrizione_element.inner_text() if await descrizione_element.count() > 0 else "Ingredienti non disponibili"
                
                # Prezzo
                prezzo_element = row.locator('[data-test-id="product-price-effective"]')
                prezzo = await prezzo_element.inner_text() if await prezzo_element.count() > 0 else "Prezzo non disponibile"
                
                # Aggiungi il piatto
                piatto = {
                    "nome": nome.strip(),
                    "ingredienti": ingredienti.strip(),
                    "prezzo": prezzo.strip()
                }
                
                piatti.append(piatto)
                
            except Exception as e:
                print(f"   ⚠️ Errore piatto {i}: {e}")
                continue
        
        return piatti
        
    except Exception as e:
        print(f"   ❌ Errore estrazione piatti: {e}")
        return []
    
# Estrazione dettagli
async def estrai_dettagli_ristorante(page, card_index):
    """Estrae i dettagli di un singolo ristorante"""
    try:
        # Clicca sul pulsante "Dettagli del locale"
        dettagli_button = page.locator("button[data-test-id='store-info-button']").nth(card_index)
        await dettagli_button.wait_for(state="visible", timeout=5000)
        await dettagli_button.click()
        
        # Aspetta che si apra la modale dei dettagli
        await page.wait_for_selector("div.store-information__modal", timeout=10000)
        await asyncio.sleep(1)
        
        dettagli = {}
        
        # Estrai l'indirizzo
        try:
            indirizzo_section = page.locator("span[data-test-id='store-information-title']:has-text('Indirizzo')").locator("..").locator("..")
            indirizzo = await indirizzo_section.locator("p[data-test-id='store-information-body']").first.inner_text()
            dettagli["indirizzo"] = indirizzo.strip()
        except:
            dettagli["indirizzo"] = "Non disponibile"
        
        # Estrai gli orari di apertura
        try:
            orari_section = page.locator("span[data-test-id='store-information-title']:has-text('Orario di apertura')").locator("..").locator("..")
            orari_elementi = orari_section.locator("p[data-test-id='store-information-body']")
            orari_count = await orari_elementi.count()
            
            orari = []
            for i in range(orari_count):
                orario = await orari_elementi.nth(i).inner_text()
                orari.append(orario.strip())
            
            dettagli["orari"] = orari
        except:
            dettagli["orari"] = []
        
        # Estrai i contatti (telefono)
        try:
            contatti_section = page.locator("span[data-test-id='store-information-title']:has-text('Contatti')").locator("..").locator("..")
            # Cerca il telefono (elemento con icona telefono)
            telefono_element = contatti_section.locator("div[data-test-id='store-information-contact-details']").locator("label").first
            telefono = await telefono_element.inner_text()
            dettagli["telefono"] = telefono.strip()
        except:
            dettagli["telefono"] = "Non disponibile"
        
        # Chiudi la modale cliccando il pulsante "Capito"
        try:
            capito_button = page.locator("button[data-test-id='store-information-button']")
            await capito_button.click()
            await asyncio.sleep(1)
        except:
            # Se non trova il pulsante, prova a premere ESC
            await page.keyboard.press("Escape")
            await asyncio.sleep(1)
        
        return dettagli
        
    except Exception as e:
        print(f"⚠️ Errore nell'estrazione dettagli: {e}")
        # Assicurati di chiudere la modale se è aperta
        try:
            await page.keyboard.press("Escape")
        except:
            pass
        return {
            "indirizzo": "Errore estrazione",
            "orari": [],
            "telefono": "Errore estrazione"
        }


async def ristorante_esiste(nome, indirizzo, ristoranti_esistenti):
    """Controlla se un ristorante con stesso nome e indirizzo esiste già nella lista"""
    nome = nome.strip().lower()
    indirizzo = indirizzo.strip().lower()

    for r in ristoranti_esistenti:
        nome_esistente = r.get("nome", "").strip().lower()
        indirizzo_esistente = r.get("indirizzo", "").strip().lower()
        if nome == nome_esistente and indirizzo == indirizzo_esistente:
            return True
    return False



async def estrai_ristoranti_da_pagina(page, ristoranti_esistenti):
    """Estrae i ristoranti dalla pagina corrente con dettagli completi"""
    # Aspetta che la pagina si carichi completamente
    await asyncio.sleep(3)
    
    # Scroll per attivare il caricamento dei ristoranti
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2)
    
    # Aspetta che la lista ristoranti sia visibile
    try:
        await page.wait_for_selector("div[data-test-id='category-store-list']", timeout=15000)
    except:
        await page.wait_for_selector("div[data-test-id='category-store-card']", timeout=15000)

    await asyncio.sleep(2)
    
    # Scroll graduale per caricare più contenuti
    for i in range(3):
        await page.evaluate("window.scrollBy(0, 800)")
        await asyncio.sleep(1)
    
    # Trova tutti i contenitori delle card dei ristoranti
    restaurant_cards = page.locator("div[data-test-id='category-store-card']")
    
    try:
        await restaurant_cards.first.wait_for(state="visible", timeout=10000)
    except:
        print("⚠️ Nessuna card trovata, provo con selettore alternativo")
        restaurant_cards = page.locator("a[data-test-id='store-item']")
        await restaurant_cards.first.wait_for(state="visible", timeout=10000)
    
    card_count = await restaurant_cards.count()
    print(f"📊 Trovate {card_count} card di ristoranti")
    
    ristoranti = []
    
    # Itera attraverso ogni card
    for i in range(card_count):
        try:
            card = restaurant_cards.nth(i)
            
            # Estrai nome e tipo (dati base)
            title_element = card.locator("h3[data-test-id='store-card-title']")
            nome = await title_element.inner_text()
            
            tag_element = card.locator("div[data-test-id='store-filter']")
            try:
                tipo = await tag_element.inner_text()
            except:
                alt_tag = card.locator(".store-card__footer__tag")
                try:
                    tipo = await alt_tag.inner_text()
                except:
                    tipo = "Non specificato"
            
            # Clicca sulla card per aprire la pagina del ristorante
            await card.click()
            await asyncio.sleep(2)
            # GESTISCI MODALE CHIUSURA PRIMA DI TUTTO
            await gestisci_modale_chiusura(page)
            
            # Estrai i dettagli del ristorante
            print(f"🔍 Estrazione dettagli per: {nome.strip()}")
            dettagli = await estrai_dettagli_ristorante(page, 0)
            
            # Controlla se il ristorante esiste già nel file, se sì salta tutto
            if await ristorante_esiste(nome, dettagli["indirizzo"], ristoranti_esistenti):
                print(f"⚠️ Ristorante già presente nel file, salto: {nome.strip()} - {dettagli['indirizzo']}")
                # Torna alla lista dei ristoranti
                await page.go_back()
                await asyncio.sleep(2)
                # Ri-attendi che la lista sia caricata
                await page.wait_for_selector("div[data-test-id='category-store-card']", timeout=10000)
                continue
            
            # ESTRAI I PIATTI
            print(f"🍽️ Estrazione piatti per: {nome.strip()}")
            piatti = await estrai_piatti_ristorante(page)
            
            # Crea l'oggetto ristorante completo
            ristorante = {
                "nome": nome.strip(),
                "tipo": tipo.strip(),
                "indirizzo": dettagli["indirizzo"],
                "orari": dettagli["orari"],
                "telefono": dettagli["telefono"],
                "piatti": piatti, 
                "data_scraping": datetime.now().isoformat()
            }
            
            ristoranti.append(ristorante)
            
            print(f"✅ {nome.strip()} - {tipo.strip()}")
            print(f"   📍 {dettagli['indirizzo']}")
            print(f"   📞 {dettagli['telefono']}")
            print(f"   🍽️ {len(piatti)} piatti estratti") 
            
            # Torna alla lista dei ristoranti
            await page.go_back()
            await asyncio.sleep(2)
            
            # Ri-attendi che la lista sia caricata
            await page.wait_for_selector("div[data-test-id='category-store-card']", timeout=10000)
            
        except Exception as e:
            print(f"⚠️ Errore nell'estrazione della card {i}: {e}")
            # Assicurati di tornare alla lista se sei in una pagina di dettaglio
            try:
                await page.go_back()
                await asyncio.sleep(2)
            except:
                pass
            continue
    
    return ristoranti

async def carica_ristoranti_esistenti(filename):
    """Carica i ristoranti già salvati per evitare duplicati"""
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Errore nel caricamento del file {filename}: {e}")
    return []

async def salva_ristoranti(ristoranti, filename):
    """Salva i ristoranti nel file JSON"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(ristoranti, f, indent=2, ensure_ascii=False)

async def estrai_ristoranti_da_zona(page, zona, ristoranti_esistenti):
    print(f"\n➡️ Elaboro zona: {zona}")

    try:
        # Naviga alla pagina principale
        url = "https://glovoapp.com/it/it/milano/cibo_1/?page=1"
        await page.goto(url, timeout=60000)
        await asyncio.sleep(2)

        # Clic sul pulsante per cambiare indirizzo
        change_btn = page.locator("div[data-test-id='address-input-button']").first
        await change_btn.wait_for(state="visible", timeout=8000)
        await change_btn.click()
        await asyncio.sleep(1)

        # Accedi all'iframe dell'indirizzo
        iframe = page.frame_locator("iframe.address-book-iframe")
        input_field = iframe.locator("input[placeholder*='Cerca'], input[aria-label='Cerca']")
        await input_field.wait_for(state="visible", timeout=10000)
        print("✅ Iframe con input caricato.")

        # Scrivi la zona
        await input_field.click()
        await input_field.fill(zona)
        print(f"⌨️ Scrivo: {zona}")
        await asyncio.sleep(2)

        # Clicca il primo suggerimento disponibile
        #first_suggestion = iframe.locator("div.ListItem_pintxo-list-item__wg8wT[data-actionable='true']").first
        first_suggestion = iframe.locator("div[data-actionable='true']").first
        await first_suggestion.wait_for(state="visible", timeout=8000)
        await first_suggestion.click()
        print("Primo suggerimento cliccato")
        
        altro_btn = iframe.locator("div.AddressTypeForm_optionContainer__RfwPt > button[name='Altro']")
        await altro_btn.wait_for(state="visible", timeout=8000)
        await altro_btn.click()
        print("Bottone 'Altro' cliccato")
        
        # Conferma indirizzo
        conferma_btn = iframe.locator("button[name='Conferma indirizzo']")
        await conferma_btn.wait_for(state="visible", timeout=8000)
        await conferma_btn.click()
        print("Bottone 'Conferma indirizzo' cliccato")
        
        # Lista per raccogliere tutti i ristoranti
        tutti_ristoranti = []
        pagina_corrente = 1
        
        while True:
            print(f"\n📄 Elaboro pagina {pagina_corrente}")
            
            # Estrai i ristoranti dalla pagina corrente (con filtro duplicati dentro)
            ristoranti_pagina = await estrai_ristoranti_da_pagina(page, ristoranti_esistenti)
            
            # Aggiungi zona e pagina ai ristoranti estratti
            for ristorante in ristoranti_pagina:
                ristorante["zona"] = zona
                ristorante["pagina"] = pagina_corrente
            
            # Aggiungi tutti i ristoranti trovati (già filtrati) alla lista finale
            tutti_ristoranti.extend(ristoranti_pagina)
            
            # Verifica se c'è una pagina successiva
            next_button = page.locator("a[data-e2e-id='pagination-controls-next-link']")
            
            try:
                if await next_button.count() > 0:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)
                    await next_button.click()
                    pagina_corrente += 1
                    await asyncio.sleep(3)
                    print(f"➡️ Passato alla pagina {pagina_corrente}")
                else:
                    print("✅ Nessuna pagina successiva trovata")
                    break
                    
            except Exception as e:
                print(f"✅ Fine paginazione: {e}")
                break
        
        print(f"\n🎉 Zona completata! Nuovi ristoranti trovati: {len(tutti_ristoranti)}")
        return tutti_ristoranti

    except Exception as e:
        print(f"❌ Errore nel trovare ristoranti a {zona}: {e}")
        await page.screenshot(path=os.path.join(debug_dir, f"errore_{zona.replace(',', '').replace(' ', '_')}.png"))
        return []

def get_prossime_zone(zone_correnti):
    """Suggerisce le prossime 2 zone da elaborare"""
    try:
        # Trova l'indice dell'ultima zona nella lista completa
        ultimo_indice = tutte_le_zone.index(zone_correnti[-1])
        
        # Calcola le prossime 2 zone
        prossime = []
        for i in range(1, 3):
            if ultimo_indice + i < len(tutte_le_zone):
                prossime.append(tutte_le_zone[ultimo_indice + i])
        
        return prossime if prossime else None
    except:
        return None

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        context = await browser.new_context(
            locale="it-IT",
            permissions=[]
        )
        page = await context.new_page()

        # File di output
        output_file = CONFIG["file_paths"]["raw_data"]
        
        # Carica ristoranti esistenti
        ristoranti_esistenti = await carica_ristoranti_esistenti(output_file)
        print(f"📊 Caricati {len(ristoranti_esistenti)} ristoranti esistenti")

        url = "https://glovoapp.com/it/it/milano/cibo_1/?page=1"
        await page.goto(url, timeout=60000)

        # Gestione cookie
        try:
            await page.locator("text='Accetta tutti'").click(timeout=8000)
            print("✅ Cookie accettati.")
        except Exception as e:
            print("❌ Cookie già gestiti o non presenti:", e)

        # Elabora ogni zona della sessione corrente
        sessione_corrente = f"Sessione con zone: {', '.join(zones)}"
        print(f"\n🚀 INIZIO {sessione_corrente}")
        
        for i, zona in enumerate(zones, 1):
            print(f"\n🎯 Inizio elaborazione zona {i}/{len(zones)}: {zona}")
            
            nuovi_ristoranti = await estrai_ristoranti_da_zona(page, zona, ristoranti_esistenti)
            
            ristoranti_esistenti.extend(nuovi_ristoranti)
            
            # Salva dopo ogni zona per non perdere i dati
            await salva_ristoranti(ristoranti_esistenti, output_file)
            
            print(f"✅ Zona {zona} completata. Totale ristoranti: {len(ristoranti_esistenti)}")
            print(f"   📊 Nuovi ristoranti da questa zona: {len(nuovi_ristoranti)}")
            
            # Pausa più lunga tra le zone per evitare rate limiting
            if i < len(zones):  # Non fare pausa dopo l'ultima zona
                print("⏳ Pausa di 30 secondi prima della prossima zona...")
                await asyncio.sleep(30)

        print(f"\n🎉 {sessione_corrente} COMPLETATA!")
        print(f"📊 Ristoranti totali nel database: {len(ristoranti_esistenti)}")
        
        # Statistiche per zona della sessione corrente
        print(f"\n📈 Statistiche per questa sessione:")
        for zona in zones:
            count = len([r for r in ristoranti_esistenti if r.get("zona") == zona])
            print(f"  {zona}: {count} ristoranti")
        
        # Prossima sessione consigliata
        if len(zones) == 2:
            prossime_zone = get_prossime_zone(zones)
            if prossime_zone:
                print(f"\n🔄 PROSSIMA SESSIONE CONSIGLIATA:")
                print(f"   zones = {prossime_zone}")
                print(f"   (Ricorda di modificare la lista 'zones' nel codice!)")

        print("\n--- RISULTATI FINALI SESSIONE ---")
        print(f"🎯 Sessione completata: {len(zones)} zone elaborate")
        print(f"📊 Ristoranti totali nel database: {len(ristoranti_esistenti)}")
        
        # Statistiche complete per tutte le zone elaborate finora
        zone_stats = {}
        for ristorante in ristoranti_esistenti:
            zona = ristorante.get("zona", "Sconosciuta")
            if zona not in zone_stats:
                zone_stats[zona] = 0
            zone_stats[zona] += 1
        
        print("\n📊 Statistiche complete (tutte le zone elaborate):")
        for zona, count in sorted(zone_stats.items()):
            print(f"  {zona}: {count} ristoranti")
        
        # Progresso complessivo
        zone_completate = len([z for z in tutte_le_zone if z in zone_stats])
        progresso = (zone_completate / len(tutte_le_zone)) * 100
        print(f"\n🏁 Progresso complessivo: {zone_completate}/{len(tutte_le_zone)} zone ({progresso:.1f}%)")
        
        # Zone rimanenti
        zone_rimanenti = [z for z in tutte_le_zone if z not in zone_stats]
        if zone_rimanenti:
            print(f"\n⏭️  Zone rimanenti da elaborare: {', '.join(zone_rimanenti)}")
        else:
            print("\n🎉 TUTTE LE ZONE COMPLETATE! 🎉")

        print(f"\n💾 Dati salvati su: {output_file}")

        input("\n🔍 Premere INVIO per chiudere il browser...")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())