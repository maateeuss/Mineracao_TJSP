"""
Processador contínuo de processos do TJSP (versão v29 - otimizado com tipo de documento pré-determinado)
- BAIXA APENAS: página principal e PDFs dentro da tabela de movimentações
- FILTRA CORRETAMENTE links de documentos: ignora href="#liberarAutoPorSenha"
- SUPORTE A PROCESSOS ANTIGOS: "Clique aqui para listar todos os eventos" + infraLinkDocumento (HTML)
- SUPORTE A PROCESSOS NOVOS: "Mais" + linkMovVincProc (PDF)
- TIPO DE DOCUMENTO PRÉ-DETERMINADO: não verifica elementos, usa informação do fluxo
- ABERTURA DE SUBLINKS EM NOVAS ABAS
- TRATAMENTO DE POPUP DE SENHA
- CLICK NO CENTRO DA TELA antes de buscar botão de download (fecha dropdown)
"""

import json
import os
import time
import sqlite3
import cv2
import numpy as np
from PIL import ImageGrab
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoAlertPresentException, SessionNotCreatedException
from urllib.parse import urljoin, unquote
import traceback
import pyautogui
import shutil

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
EXTRACTION_DIR = r"D:\TJSP\extracao_20260325_223530"
BASE_SAVE_DIR = r"D:\TJSP\TJSP_FILES"
DB_PATH = os.path.join(BASE_SAVE_DIR, "extraction_tracker.db")
CHROME_PROFILE_DIR = r"C:\Users\Administrator\Desktop\Mineração-TJSP\chrome_profile"
CHROME_DRIVER_PATH = r"C:\Users\Administrator\Desktop\Mineração-TJSP\webdrivers\chromedriver.exe"
DOWNLOAD_PATH = r"C:\Users\Administrator\Downloads"

INSTANCIA = "SP"
TIMEOUT_TURNSTILE = 60
TIMEOUT_PAGE_LOAD = 30
TIMEOUT_EVENTOS_LINK = 5
TIMEOUT_TITLE_CHANGE = 20
TIMEOUT_DOWNLOAD = 30
TIMEOUT_POPUP = 2
TIMEOUT_NETWORK_IDLE = 10
SLEEP_BETWEEN_PROCESSOS = 2
SCAN_INTERVAL = 60
MAX_PROCESSOS_POR_CICLO = 10

SUCCESS_IMG_LIGHT = r"images\sucesso.PNG"
SUCCESS_IMG_DARK = r"images\sucesso_dark.PNG"
CHECK_IMG = r"images\check.PNG"
DOWNLOAD_PDF_IMG_LIGHT = r"images\download_pdf.PNG"
DOWNLOAD_PDF_IMG_DARK = r"images\download_pdf_dark.PNG"
SAVE_AS_IMG = r"images\save_as.PNG"
SAVE_BUTON = r"images\save_btn.PNG"
PRINT_BUTTON_IMG = r"images\print_button.PNG"
CANCEL_POPUP_IMG = r"images\cancel_popup.PNG"

TITLE_ESAJ = "Portal de Serviços e-SAJ"
TITLE_DETALHE = "Detalhe do Processo"
BASE_URL = "https://esaj.tjsp.jus.br"
SEARCH_URL = "eproc-consulta.tjsp.jus.br"

# ============================================================================
# VALIDAÇÕES INICIAIS
# ============================================================================
if not os.path.exists(CHROME_DRIVER_PATH):
    raise FileNotFoundError(f"ChromeDriver não encontrado: {CHROME_DRIVER_PATH}")

if not os.path.exists(CHROME_PROFILE_DIR):
    os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)

os.makedirs(BASE_SAVE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

# ============================================================================
# BANCO DE DADOS
# ============================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS processos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    numero_processo TEXT UNIQUE,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS subdocumentos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    processo_id INTEGER,
                    url TEXT,
                    local_filename TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(processo_id) REFERENCES processos(id)
                 )''')
    conn.commit()
    conn.close()

init_db()
print("[DB] Inicializado.")

def create_processo(numero):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO processos (numero_processo, status) VALUES (?, 'pending')", (numero,))
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        c.execute("SELECT id FROM processos WHERE numero_processo = ?", (numero,))
        return c.fetchone()[0]
    finally:
        conn.close()

def update_processo_status(processo_id, status, error_msg=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if error_msg:
        c.execute("UPDATE processos SET status = ?, updated_at = CURRENT_TIMESTAMP, error_message = ? WHERE id = ?",
                  (status, error_msg, processo_id))
    else:
        c.execute("UPDATE processos SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                  (status, processo_id))
    conn.commit()
    conn.close()

def add_subdocumento(processo_id, url):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO subdocumentos (processo_id, url, status) VALUES (?, ?, 'pending')", (processo_id, url))
    conn.commit()
    subdoc_id = c.lastrowid
    conn.close()
    return subdoc_id

def update_subdocumento_status(subdoc_id, status, filename=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if filename:
        c.execute("UPDATE subdocumentos SET status = ?, local_filename = ? WHERE id = ?", (status, filename, subdoc_id))
    else:
        c.execute("UPDATE subdocumentos SET status = ? WHERE id = ?", (status, subdoc_id))
    conn.commit()
    conn.close()

def is_subdoc_processed(processo_id, url):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM subdocumentos WHERE processo_id = ? AND url = ?", (processo_id, url))
    row = c.fetchone()
    conn.close()
    return row is not None

# ============================================================================
# DETECÇÃO DE IMAGEM
# ============================================================================
def detect_image(screenshot, template_path, threshold=0.8):
    img_rgb = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
    img_gray = cv2.cvtColor(img_rgb, cv2.COLOR_BGR2GRAY)
    template = cv2.imread(template_path, cv2.IMREAD_GRAYSCALE)
    if template is None:
        return None
    w, h = template.shape[::-1]
    res = cv2.matchTemplate(img_gray, template, cv2.TM_CCOEFF_NORMED)
    loc = np.where(res >= threshold)
    if len(loc[0]) == 0:
        return None
    y, x = loc[0][0], loc[1][0]
    return (x + w // 2, y + h // 2)

def click_image(template_path, timeout=15, threshold=0.9):
    """Aguarda imagem e clica via pyautogui."""
    print(f"[IMG] {template_path} (threshold={threshold})")
    start = time.time()
    while time.time() - start < timeout:
        screenshot = ImageGrab.grab()
        coords = detect_image(screenshot, template_path, threshold)
        if coords:
            pyautogui.click(coords[0], coords[1])
            print(f"[IMG] OK {coords}")
            time.sleep(0.2)
            return True
        time.sleep(0.1)
    print(f"[IMG] Não encontrado")
    return False

def wait_for_image(template_path, timeout=15, threshold=0.9):
    """Aguarda imagem aparecer, sem clicar."""
    start = time.time()
    while time.time() - start < timeout:
        screenshot = ImageGrab.grab()
        coords = detect_image(screenshot, template_path, threshold)
        if coords:
            return True
        time.sleep(0.1)
    return False


# ============================================================================
# DOWNLOAD DE PDF (OTIMIZADO - TIPO PRÉ-DETERMINADO)
# ============================================================================
def handle_save_as_dialog(filename):
    """Aguarda a janela 'Salvar como' do Chrome e interage."""
    if not wait_for_image(SAVE_AS_IMG, timeout=10, threshold=0.8):
        print("[DOWNLOAD] Janela 'Salvar como' não apareceu")
        return False
    time.sleep(0.5)
    pyautogui.hotkey('ctrl', 'a')
    time.sleep(0.2)
    pyautogui.write(filename)
    time.sleep(0.2)
    pyautogui.press('enter')
    print(f"[DOWNLOAD] Nome enviado: {filename}")
    return True

def wait_for_pdf_download(filename, timeout=TIMEOUT_DOWNLOAD):
    """Aguarda PDF aparecer na pasta Downloads."""
    start = time.time()
    while time.time() - start < timeout:
        filepath = os.path.join(DOWNLOAD_PATH, filename)
        if os.path.exists(filepath):
            try:
                initial_size = os.path.getsize(filepath)
                time.sleep(1)
                final_size = os.path.getsize(filepath)
                if initial_size == final_size and final_size > 0:
                    return filepath
            except:
                pass
        time.sleep(0.5)
    return None

def click_center_screen():
    """Clica no centro da tela para fechar dropdown de downloads do Chrome."""
    screen_width, screen_height = pyautogui.size()
    center_x = screen_width // 2
    center_y = screen_height // 2
    pyautogui.click(center_x, center_y)
    print(f"[CLICK] Centro da tela clicado ({center_x}, {center_y}) - dropdown fechado")
    time.sleep(0.3)

def download_pdf_via_click(driver, subdoc_id, process_folder, numero_processo, doc_type="pdf"):
    """
    Baixa documento PDF ou HTML.
    doc_type: "pdf" (processos novos) ou "html" (processos antigos) - já determinado pelo fluxo.
    """
    try:
        filename = f"sub_{subdoc_id}_{numero_processo.replace('/', '_')}.pdf"
        target_filepath = os.path.join(process_folder, filename)

        print(f"[PDF] Tipo de documento: {doc_type.upper()}")
        
        # Aguarda um pouco para a página carregar
        time.sleep(0.5)

        # Click no centro da tela para fechar dropdown do Chrome
        print("[PDF] Fechando dropdown de downloads (click no centro)...")
        click_center_screen()

        # === PROCESSAMENTO POR TIPO (SEM VERIFICAÇÃO DE ELEMENTOS) ===
        if doc_type == "html":
            # Documento HTML (processos antigos): usa botão printButtom
            print("[PDF] Documento HTML - Buscando botão de imprimir (printButtom)...")
            time.sleep(0.5)
            
            print_btn_clicked = False
            
            # === TENTATIVA 1: Selenium com scroll ===
            try:
                print("[PDF] Tentativa 1: Selenium com scroll...")
                print_btn = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.ID, "printButtom"))
                )
                # Scroll até o elemento
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", print_btn)
                time.sleep(0.5)
                # Click via JavaScript
                driver.execute_script("arguments[0].click();", print_btn)
                print("[PDF] Botão printButtom clicado via Selenium.")
                print_btn_clicked = True
            except TimeoutException:
                print("[PDF] printButtom não encontrado via Selenium.")
            except Exception as e:
                print(f"[PDF] Erro Selenium: {e}")
            
            # === TENTATIVA 2: Fallback por imagem ===
            if not print_btn_clicked:
                print("[PDF] Tentativa 2: Buscando botão de imprimir via imagem...")
                if os.path.exists(PRINT_BUTTON_IMG):
                    if click_image(PRINT_BUTTON_IMG, timeout=5, threshold=0.8):
                        print("[PDF] Botão de imprimir clicado via imagem.")
                        print_btn_clicked = True
                    else:
                        print("[PDF] Botão de imprimir não encontrado via imagem.")
                else:
                    print("[PDF] Imagem print_button.PNG não encontrada. Usando Ctrl+P...")
            
            # === TENTATIVA 3: Fallback Ctrl+P ===
            if not print_btn_clicked:
                print("[PDF] Tentativa 3: Usando Ctrl+P...")
                pyautogui.hotkey('ctrl', 'p')
                print_btn_clicked = True
            
            time.sleep(1.5)  # Aguarda diálogo de print abrir
            
            # === CLICA NO BOTÃO SALVAR (SAVE_BUTON) ===
            print("[PDF] Buscando botão Salvar (save_btn.PNG)...")
            if click_image(SAVE_BUTON, timeout=10, threshold=0.8):
                print("[PDF] Botão Salvar clicado via imagem.")
            else:
                print("[PDF] Botão Salvar não encontrado. Tentando Enter...")
                pyautogui.press('enter')
            
            time.sleep(1)  # Aguarda diálogo Salvar Como aparecer
            # ===========================================================
            
        elif doc_type == "pdf":
            # Documento PDF (processos novos): busca botão de download via imagem
            download_img = None
            if os.path.exists(DOWNLOAD_PDF_IMG_LIGHT) and wait_for_image(DOWNLOAD_PDF_IMG_LIGHT, timeout=3, threshold=0.9):
                download_img = DOWNLOAD_PDF_IMG_LIGHT
            elif os.path.exists(DOWNLOAD_PDF_IMG_DARK) and wait_for_image(DOWNLOAD_PDF_IMG_DARK, timeout=3, threshold=0.9):
                download_img = DOWNLOAD_PDF_IMG_DARK

            if not download_img:
                print("[PDF] Botão de download não encontrado")
                return None

            print("[PDF] Clicando em download...")
            if not click_image(download_img, timeout=3, threshold=0.9):
                print("[PDF] Falha ao clicar no botão")
                return None
        # ================================================================

        # === DIÁLOGO SALVAR COMO (COMUM PARA HTML E PDF) ===
        if not handle_save_as_dialog(filename):
            return None

        downloaded_file = wait_for_pdf_download(filename)
        if not downloaded_file:
            print("[PDF] Timeout no download")
            return None

        if os.path.exists(target_filepath):
            os.remove(target_filepath)
        shutil.move(downloaded_file, target_filepath)
        print(f"[PDF] Salvo: {target_filepath}")
        return filename

    except Exception as e:
        print(f"[PDF] Erro: {e}")
        traceback.print_exc()
        return None

# ============================================================================
# CRIAÇÃO DO DRIVER
# ============================================================================
def create_driver():
    print("[DRIVER] Criando...")
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={CHROME_PROFILE_DIR}")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    # Configurações para permitir popups/novas abas
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-features=TranslateUI")
    
    prefs = {
        "download.default_directory": DOWNLOAD_PATH,
        "download.prompt_for_download": True,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.popups": 1,
        "profile.default_content_settings.popups": 0
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = None
    try:
        driver = uc.Chrome(
            options=options,
            driver_executable_path=CHROME_DRIVER_PATH,
            use_subprocess=False
        )
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.implicitly_wait(3)
        time.sleep(1)
        print("[DRIVER] OK")
        return driver
    except Exception as e:
        print(f"[DRIVER] Erro: {e}")
        if driver:
            try:
                driver.quit()
            except:
                pass
        raise

def accept_alert_if_present(driver, timeout=1):
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        alert.accept()
        return True
    except:
        return False

def click_element_via_js(driver, element):
    driver.execute_script("arguments[0].click();", element)

def save_html_content(driver, filepath):
    html = driver.page_source
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[HTML] Salvo: {filepath}")

# ============================================================================
# EXPANSÃO E EXTRAÇÃO DE LINKS (RETORNA TIPO DE DOCUMENTO)
# ============================================================================
def expand_movements(driver):
    """
    Clica no link de expansão de movimentações.
    Retorna: "pdf" (processos novos) ou "html" (processos antigos)
    """
    doc_type = "pdf"  # Default
    
    # Tentativa 1: Link "Mais" (processos novos) → PDF
    try:
        mais_link = WebDriverWait(driver, TIMEOUT_EVENTOS_LINK).until(
            EC.element_to_be_clickable((By.ID, "linkmovimentacoes"))
        )
        click_element_via_js(driver, mais_link)
        print("[MOV] Expandindo via 'Mais' (linkmovimentacoes)...")
        WebDriverWait(driver, TIMEOUT_EVENTOS_LINK).until(
            EC.presence_of_element_located((By.ID, "tabelaTodasMovimentacoes"))
        )
        time.sleep(0.5)
        doc_type = "pdf"
        print(f"[MOV] Tipo de documento: {doc_type.upper()} (processo novo)")
        return doc_type
    except TimeoutException:
        print("[MOV] Link 'Mais' não encontrado. Tentando link antigo...")
    
    # Tentativa 2: Link "Clique aqui para listar todos os eventos" (processos antigos) → HTML
    try:
        eventos_link = WebDriverWait(driver, TIMEOUT_EVENTOS_LINK).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'listar todos os eventos')]"))
        )
        click_element_via_js(driver, eventos_link)
        print("[MOV] Expandindo via 'Clique aqui para listar todos os eventos'...")
        time.sleep(1)
        doc_type = "html"
        print(f"[MOV] Tipo de documento: {doc_type.upper()} (processo antigo)")
        return doc_type
    except TimeoutException:
        print("[MOV] Link de eventos antigos não encontrado.")
    
    # Tentativa 3: Fallback por texto "Mais" genérico → PDF
    try:
        mais_link = WebDriverWait(driver, TIMEOUT_EVENTOS_LINK).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(@class,'unj-link-collapse') and contains(text(),'Mais')]"))
        )
        click_element_via_js(driver, mais_link)
        print("[MOV] Expandindo movimentações (fallback por texto)...")
        WebDriverWait(driver, TIMEOUT_EVENTOS_LINK).until(
            EC.presence_of_element_located((By.ID, "tabelaTodasMovimentacoes"))
        )
        time.sleep(0.5)
        doc_type = "pdf"
        print(f"[MOV] Tipo de documento: {doc_type.upper()} (fallback)")
        return doc_type
    except TimeoutException:
        print("[MOV] Nenhum link de expansão encontrado. Prosseguindo com tabela visível.")
        return doc_type

def get_movements_elements(driver, doc_type):
    """
    Retorna lista de elementos <a> que representam documentos.
    doc_type: "pdf" (linkMovVincProc) ou "html" (infraLinkDocumento)
    """
    elements = []
    
    # Processos novos: linkMovVincProc (PDF)
    if doc_type == "pdf":
        for table_id in ["tabelaUltimasMovimentacoes", "tabelaTodasMovimentacoes"]:
            try:
                table = driver.find_element(By.ID, table_id)
                links = table.find_elements(By.CLASS_NAME, "linkMovVincProc")
                for a in links:
                    href = a.get_attribute("href")
                    if not href:
                        continue
                    if href.startswith('#') or 'liberarAutoPorSenha' in href:
                        continue
                    if a not in elements:
                        elements.append(a)
            except:
                continue
    
    # Processos antigos: infraLinkDocumento (HTML)
    elif doc_type == "html":
        try:
            old_links = driver.find_elements(By.CLASS_NAME, "infraLinkDocumento")
            print(f"[MOV] Encontrados {len(old_links)} elementos infraLinkDocumento (processos antigos)")
            for a in old_links:
                href = a.get_attribute("href")
                if not href or href.startswith('#') or 'liberarAutoPorSenha' in href:
                    continue
                if a not in elements:
                    elements.append(a)
        except:
            pass
    
    print(f"[MOV] Total: {len(elements)} elementos de documento com URLs válidas.")
    return elements

# ============================================================================
# PROCESSAMENTO DE SUBDOCUMENTOS (COM TIPO DE DOCUMENTO)
# ============================================================================
def process_subdocument(driver, parent_processo_id, element, process_folder, numero_processo, doc_type):
    """
    Recebe um elemento <a>, abre URL diretamente em nova aba, processa e fecha.
    doc_type: "pdf" ou "html" - já determinado pelo fluxo.
    """
    try:
        url = element.get_attribute("href")
        
        if is_subdoc_processed(parent_processo_id, url):
            print(f"[SUBDOC] Já processado, pulando: {url[-60:]}")
            return None

        subdoc_id = add_subdocumento(parent_processo_id, url)
        print(f"[SUBDOC] ID:{subdoc_id} - {url[-80:]}")

        original_handles = driver.window_handles
        print(f"[SUBDOC] Handles antes: {len(original_handles)}")
        
        # Abre via JavaScript direto na URL
        escaped_url = url.replace("'", "\\'").replace('"', '\\"')
        driver.execute_script(f"window.open('{escaped_url}', '_blank');")
        print(f"[SUBDOC] window.open executado")
        
        # Aguarda nova aba abrir
        start = time.time()
        while len(driver.window_handles) <= len(original_handles) and (time.time() - start) < 5:
            time.sleep(0.2)
        
        new_handles = driver.window_handles
        print(f"[SUBDOC] Handles depois: {len(new_handles)}")
        
        if len(new_handles) <= len(original_handles):
            print("[SUBDOC] window.open falhou. Tentando click via JS...")
            driver.execute_script("arguments[0].click();", element)
            start = time.time()
            while len(driver.window_handles) <= len(original_handles) and (time.time() - start) < 5:
                time.sleep(0.2)
            new_handles = driver.window_handles
            print(f"[SUBDOC] Handles após click: {len(new_handles)}")
        
        if len(new_handles) <= len(original_handles):
            print("[SUBDOC] ERRO: Nenhuma nova aba aberta após tentativas.")
            update_subdocumento_status(subdoc_id, "error")
            return None

        driver.switch_to.window(new_handles[-1])
        WebDriverWait(driver, TIMEOUT_PAGE_LOAD).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)
        print(f"[SUBDOC] Título da aba: {driver.title[:80]}")

        # Passa o doc_type para a função de download (SEM VERIFICAÇÃO DE ELEMENTOS)
        pdf_filename = download_pdf_via_click(driver, subdoc_id, process_folder, numero_processo, doc_type=doc_type)

        if pdf_filename:
            update_subdocumento_status(subdoc_id, "completed", pdf_filename)
            print(f"[SUBDOC] PDF OK: {pdf_filename}")
        else:
            print(f"[SUBDOC] Nenhum PDF identificado ou falha no download.")
            update_subdocumento_status(subdoc_id, "completed")

        driver.close()
        driver.switch_to.window(original_handles[0])
        return pdf_filename

    except Exception as e:
        print(f"[SUBDOC] Erro: {e}")
        traceback.print_exc()
        update_subdocumento_status(subdoc_id, "error")
        try:
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
        except:
            pass
        return None

# ============================================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================================
def process_processo(numero_processo):
    print(f"\n=== {numero_processo} ===")
    processo_id = create_processo(numero_processo)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT status FROM processos WHERE id = ?", (processo_id,))
    status = c.fetchone()[0]
    conn.close()

    if status in ("completed", "error"):
        print(f"Pulado: {status}")
        return

    update_processo_status(processo_id, "processing")
    driver = None

    try:
        driver = create_driver()

        print("[NAV] Acessando...")
        
        # === TENTATIVAS DE CONEXÃO COM DETECÇÃO DE ERRO ===
        max_tentativas = 3
        tentativa = 0
        conexao_ok = False
        
        while tentativa < max_tentativas and not conexao_ok:
            tentativa += 1
            print(f"[NAV] Tentativa {tentativa}/{max_tentativas}...")
            
            try:
                driver.get("https://eproc-consulta.tjsp.jus.br/consulta_1g/externo_controlador.php?acao=tjsp@consulta_unificada_publica/consultar")
                time.sleep(2)  # Aguarda página carregar
                
                # Verifica se é página de erro de conexão
                page_source = driver.page_source.lower()
                erro_detectado = any([
                    "não é possível acessar esse site" in page_source,
                    "err_connection_timed_out" in page_source,
                    "demorou muito para responder" in page_source,
                    "verificar a conexão" in page_source,
                    "504 gateway" in page_source,
                    "502 bad gateway" in page_source
                ])
                
                # Também verifica pelo título da página de erro do Chrome
                if "não é possível acessar" in driver.title.lower() or "err_" in driver.title.lower():
                    erro_detectado = True
                
                if erro_detectado:
                    print(f"[NAV] Erro de conexão detectado na tentativa {tentativa}.")
                    if tentativa < max_tentativas:
                        print("[NAV] Tentando refresh...")
                        driver.refresh()
                        time.sleep(1)
                    else:
                        print("[NAV] Máximo de tentativas atingido.")
                else:
                    # Verifica se o formulário carregou corretamente
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.ID, "txtNumProcesso"))
                        )
                        conexao_ok = True
                        print("[NAV] Página carregada com sucesso.")
                    except TimeoutException:
                        print(f"[NAV] Formulário não carregou na tentativa {tentativa}.")
                        if tentativa < max_tentativas:
                            driver.refresh()
                            time.sleep(1)
                
            except Exception as e:
                print(f"[NAV] Erro na tentativa {tentativa}: {e}")
                if tentativa < max_tentativas:
                    print("[NAV] Tentando refresh...")
                    driver.refresh()
                    time.sleep(1)
        
        if not conexao_ok:
            raise Exception("Falha de conexão após múltiplas tentativas")
        # ============================================================

        print("[LOAD] Aguardando formulário...")
        WebDriverWait(driver, TIMEOUT_PAGE_LOAD).until(
            EC.presence_of_element_located((By.ID, "txtNumProcesso"))
        )
        time.sleep(1)


        if os.path.exists(CHECK_IMG):
            click_image(CHECK_IMG, timeout=10, threshold=0.75)
            time.sleep(1)

        print("[FORM] Preenchendo...")
        input_num = driver.find_element(By.ID, "txtNumProcesso")
        input_num.clear()
        input_num.send_keys(numero_processo)
        time.sleep(0.3)

        select_inst = driver.find_element(By.ID, "selInstancia")
        select_inst.find_element(By.XPATH, f"option[@value='{INSTANCIA}']").click()
        time.sleep(0.3)

        print("[FORM] Enviando...")
        btn_consultar = driver.find_element(By.ID, "sbmNovo")
        click_element_via_js(driver, btn_consultar)
        accept_alert_if_present(driver, timeout=2)

        # === DETECÇÃO DE FLUXO DISTINGUIDA (SEARCH → DETALHE) ===
        print("[FLOW] Aguardando transição search → detail...")
        flow_ok = wait_for_page_transition(driver, timeout=TIMEOUT_TITLE_CHANGE)

        if not flow_ok:
            print("[CAPTCHA] Verificando ícone...")
            start_time = time.time()
            while time.time() - start_time < TIMEOUT_TURNSTILE:
                screenshot = ImageGrab.grab()
                if detect_image(screenshot, SUCCESS_IMG_LIGHT) or detect_image(screenshot, SUCCESS_IMG_DARK):
                    print("✅ Captcha OK")
                    flow_ok = wait_for_page_transition(driver, timeout=TIMEOUT_TITLE_CHANGE)
                    break
                time.sleep(0.5)

        if not flow_ok:
            raise Exception("Timeout: página não carregou")

        print("[NETWORK] Aguarda carregamento completo...")
        time.sleep(TIMEOUT_NETWORK_IDLE // 2)

        # === EXPANSÃO RETORNA TIPO DE DOCUMENTO ===
        doc_type = expand_movements(driver)

        process_folder = os.path.join(BASE_SAVE_DIR, numero_processo.replace("/", "_"))
        os.makedirs(process_folder, exist_ok=True)
        main_filepath = os.path.join(process_folder, "principal.html")
        save_html_content(driver, main_filepath)

        # === PASSA doc_type PARA get_movements_elements ===
        elements = get_movements_elements(driver, doc_type)

        for element in elements:
            # === PASSA doc_type PARA process_subdocument ===
            process_subdocument(driver, processo_id, element, process_folder, numero_processo, doc_type)
            time.sleep(SLEEP_BETWEEN_PROCESSOS)

        if os.path.exists(process_folder) and not os.listdir(process_folder):
            os.rmdir(process_folder)
            print(f"[CLEAN] Pasta vazia removida: {process_folder}")

        update_processo_status(processo_id, "completed")
        print(f"✅ {numero_processo}")

    except SessionNotCreatedException as e:
        error_msg = f"ChromeDriver: {e}"
        print(error_msg)
        update_processo_status(processo_id, "error", error_msg)
    except Exception as e:
        error_msg = str(e)
        print(f"Erro: {error_msg}")
        traceback.print_exc()
        update_processo_status(processo_id, "error", error_msg)
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        time.sleep(SLEEP_BETWEEN_PROCESSOS)

# ============================================================================
# DETECÇÃO DE TRANSIÇÃO DE PÁGINA (SEARCH → DETALHE)
# ============================================================================
def wait_for_page_transition(driver, timeout=TIMEOUT_TITLE_CHANGE):
    """
    Detecta transição de página search → detail.
    Fluxo 1: Search page (sem title ou URL com 'consulta_1g') → Detail page (com title)
    Fluxo 2: Search page → Detail page (sem title no head, mas URL muda)
    """
    start = time.time()
    
    # Captura estado inicial
    try:
        initial_url = driver.current_url
        initial_title = driver.title
        print(f"[FLOW] URL inicial: {initial_url[:80]}...")
        print(f"[FLOW] Title inicial: '{initial_title}'")
    except:
        initial_url = ""
        initial_title = ""
    
    # Verifica se já está na página de detalhe (title já mudou antes da verificação)
    if TITLE_ESAJ in initial_title or TITLE_DETALHE in initial_title:
        print(f"[FLOW] Já está na página de detalhe (title: '{initial_title}')")
        return True
    
    # Aguarda transição
    while time.time() - start < timeout:
        try:
            current_url = driver.current_url
            current_title = driver.title
            
            # Verifica mudança de URL (search → detail)
            if SEARCH_URL in initial_url and SEARCH_URL not in current_url:
                print(f"[FLOW] URL mudou: search → detail")
                return True
            
            # Verifica mudança de title para títulos esperados
            if current_title and current_title != initial_title:
                if TITLE_ESAJ in current_title or TITLE_DETALHE in current_title:
                    print(f"[FLOW] Title mudou para: '{current_title}'")
                    return True
            
            # Verifica elementos específicos da página de detalhe
            try:
                detail_indicators = [
                    (By.ID, "tabelaUltimasMovimentacoes"),
                    (By.ID, "tabelaTodasMovimentacoes"),
                    (By.CLASS_NAME, "linkMovVincProc"),
                    (By.CLASS_NAME, "infraLinkDocumento"),
                    (By.XPATH, "//a[contains(text(),'listar todos os eventos')]"),
                    (By.XPATH, "//a[contains(text(),'Mais')]")
                ]
                for by_type, value in detail_indicators:
                    try:
                        element = driver.find_element(by_type, value)
                        if element.is_displayed():
                            print(f"[FLOW] Elemento de detalhe encontrado: {value}")
                            return True
                    except:
                        continue
            except:
                pass
            
        except:
            pass
        time.sleep(0.3)
    
    print(f"[FLOW] Timeout: transição não detectada")
    return False

# ============================================================================
# LEITURA DOS JSONS
# ============================================================================
def get_unprocessed_processes_from_extraction():
    if not os.path.exists(EXTRACTION_DIR):
        return []

    unprocessed = set()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT numero_processo FROM processos WHERE status IN ('completed', 'error')")
    completed_or_error = {row[0] for row in c.fetchall()}
    conn.close()

    files = [f for f in os.listdir(EXTRACTION_DIR) if f.startswith("batch_") and f.endswith(".json")]
    print(f"[SCAN] {len(files)} arquivos")

    for file in files:
        filepath = os.path.join(EXTRACTION_DIR, file)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            for hit in data:
                numero = hit.get("_source", {}).get("numeroProcesso")
                if numero and numero not in completed_or_error:
                    unprocessed.add(numero)
        except Exception as e:
            print(f"Erro: {filepath}: {e}")

    return list(unprocessed)

# ============================================================================
# LOOP PRINCIPAL
# ============================================================================
def main_loop():
    print("Iniciando...")
    while True:
        processos = get_unprocessed_processes_from_extraction()
        if processos:
            to_process = processos[:MAX_PROCESSOS_POR_CICLO]
            print(f"\n{len(processos)} novos. Processando {len(to_process)}...")
            for num in to_process:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT status FROM processos WHERE numero_processo = ?", (num,))
                row = c.fetchone()
                conn.close()
                if row and row[0] in ("completed", "error"):
                    continue
                process_processo(num)
        else:
            print(f"Aguardando {SCAN_INTERVAL}s...")
            time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main_loop()