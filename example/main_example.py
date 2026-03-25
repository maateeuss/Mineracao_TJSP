# pip install pyautogui opencv-python numpy Pillow undetected-chromedriver selenium

import json
import time
import os
import cv2
import numpy as np
from PIL import ImageGrab
import pyautogui
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoAlertPresentException
import threading

# ------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------
JSON_FILE = "datajud_response_sample.json"
PDF_SAVE_PATH = r"C:\Users\Administrator\Desktop\Mineração\pdfs"
USER_DATA_DIR = r"C:\Users\Administrator\Desktop\Mineração\chrome_profile"
INSTANCIA = "SP"
TIMEOUT_TURNSTILE = 60
TIMEOUT_SAVE_BUTTON = 30
TIMEOUT_DOWNLOAD = 30

# Caminhos das imagens de referência
SUCCESS_IMG_LIGHT = r"images\sucesso.PNG"
SUCCESS_IMG_DARK = r"images\sucesso_dark.PNG"

# Pasta de downloads do Windows
DOWNLOAD_PATH = r"C:\Users\Administrator\Downloads"

# ------------------------------------------------------------
# 1. Carregar JSON
# ------------------------------------------------------------
try:
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Erro: Arquivo {JSON_FILE} não encontrado.")
    exit(1)

try:
    processo_numero = data["hits"]["hits"][0]["_source"]["numeroProcesso"]
    print(f"Número do processo encontrado: {processo_numero}")
except (KeyError, IndexError):
    print("Erro ao extrair número do processo.")
    exit(1)

os.makedirs(PDF_SAVE_PATH, exist_ok=True)

# ------------------------------------------------------------
# 2. Funções de detecção e clique via imagem
# ------------------------------------------------------------
def detect_image(screenshot, template_path, threshold=0.8):
    """Retorna (coordenadas x, y) do centro do template encontrado, ou None."""
    img_rgb = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
    img_gray = cv2.cvtColor(img_rgb, cv2.COLOR_BGR2GRAY)
    template = cv2.imread(template_path, cv2.IMREAD_GRAYSCALE)
    if template is None:
        print(f"⚠️ Template não encontrado: {template_path}")
        return None
    w, h = template.shape[::-1]
    res = cv2.matchTemplate(img_gray, template, cv2.TM_CCOEFF_NORMED)
    loc = np.where(res >= threshold)
    if len(loc[0]) == 0:
        return None
    y, x = loc[0][0], loc[1][0]
    center_x = x + w // 2
    center_y = y + h // 2
    return (center_x, center_y)

def wait_for_image(template_path, timeout=TIMEOUT_SAVE_BUTTON, threshold=0.8):
    """Aguarda até que a imagem apareça na tela e retorna suas coordenadas."""
    print(f"Aguardando imagem: {template_path}")
    start_time = time.time()
    while time.time() - start_time < timeout:
        screenshot = ImageGrab.grab()
        coords = detect_image(screenshot, template_path, threshold)
        if coords:
            print(f"✅ Imagem detectada em {coords}")
            return coords
        time.sleep(0.5)
    print(f"❌ Imagem não detectada dentro do tempo limite.")
    return None

def click_at(coords):
    """Move o mouse para as coordenadas e clica."""
    if coords:
        pyautogui.moveTo(coords[0], coords[1], duration=0.5)
        pyautogui.click()
        return True
    return False

# ------------------------------------------------------------
# 3. Configurar Chrome (sem definir pasta de download, pois usaremos diálogo)
# ------------------------------------------------------------
options = uc.ChromeOptions()
options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")

# Não definimos prefs de download para que o Chrome use a pasta padrão (Downloads)
# Mas podemos manter outras preferências, como evitar prompt de download?
# Como usaremos o diálogo, não precisamos configurar.

driver = uc.Chrome(options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# ------------------------------------------------------------
# 4. Navegar e enviar formulário
# ------------------------------------------------------------
url = "https://eproc-consulta.tjsp.jus.br/consulta_1g/externo_controlador.php?acao=tjsp@consulta_unificada_publica/consultar"
driver.get(url)

wait = WebDriverWait(driver, 20)

input_processo = wait.until(EC.presence_of_element_located((By.ID, "txtNumProcesso")))
input_processo.clear()
input_processo.send_keys(processo_numero)

select_inst = driver.find_element(By.ID, "selInstancia")
select_inst.find_element(By.XPATH, f"option[@value='{INSTANCIA}']").click()

driver.find_element(By.ID, "sbmNovo").click()
print("Formulário enviado. Aguardando verificação do Cloudflare Turnstile...")

# ------------------------------------------------------------
# 5. Aceitar alerta se aparecer
# ------------------------------------------------------------
def accept_alert_if_present(driver, timeout=5):
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        print(f"Alerta encontrado: {alert.text}")
        alert.accept()
        return True
    except (TimeoutException, NoAlertPresentException):
        return False

accept_alert_if_present(driver, timeout=5)

# ------------------------------------------------------------
# 6. Detecção visual do ícone de sucesso
# ------------------------------------------------------------
print("Aguardando o ícone de sucesso aparecer na tela...")
start_time = time.time()
success_detected = False
while time.time() - start_time < TIMEOUT_TURNSTILE:
    screenshot = ImageGrab.grab()
    if (detect_image(screenshot, SUCCESS_IMG_LIGHT, threshold=0.8) or
        detect_image(screenshot, SUCCESS_IMG_DARK, threshold=0.8)):
        success_detected = True
        print("✅ Ícone de sucesso detectado na tela!")
        break
    time.sleep(1)

if not success_detected:
    print("❌ Tempo esgotado: ícone de sucesso não detectado.")
    driver.quit()
    exit(1)

time.sleep(2)

# ------------------------------------------------------------
# 7. Re-submeter formulário se necessário
# ------------------------------------------------------------
try:
    consultar_btn = driver.find_element(By.ID, "sbmNovo")
    if consultar_btn.is_displayed():
        print("Botão 'Consultar' ainda visível. Clicando novamente...")
        consultar_btn.click()
        accept_alert_if_present(driver, timeout=3)
        time.sleep(2)
except:
    pass

# ------------------------------------------------------------
# 8. Clicar no link "Clique aqui para listar todos os eventos" com JavaScript
# ------------------------------------------------------------
print("Procurando o link para listar todos os eventos...")
try:
    link_eventos = wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'Clique aqui para listar todos os eventos')]")))
    print("Link encontrado. Rolando até ele...")
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_eventos)
    time.sleep(1)
    print("Clicando no link via JavaScript...")
    driver.execute_script("arguments[0].click();", link_eventos)
    print("Clique executado. Aguardando recarregamento da página...")
    # Aguarda a página recarregar
    time.sleep(3)  # pausa simples para garantir o recarregamento
except TimeoutException:
    print("❌ Link de eventos não encontrado. Prosseguindo...")

# ------------------------------------------------------------
# 9. Clicar no botão "Imprimir" via JavaScript e tratar janela de salvamento
# ------------------------------------------------------------
print("Aguardando o botão 'Imprimir'...")
try:
    imprimir_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='Imprimir']")))
    print("Botão 'Imprimir' (input) encontrado.")
except TimeoutException:
    try:
        imprimir_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//button[contains(text(),'Imprimir')]")))
        print("Botão 'Imprimir' (button) encontrado.")
    except:
        print("❌ Nenhum botão de impressão encontrado.")
        driver.quit()
        exit(1)

# Função para interagir com a janela de salvamento
def handle_save_dialog(filename):
    # Primeiro, aguarda a janela aparecer e pressiona Enter para abrir a janela de escolha de pasta?
    # O seu ajuste anterior:
    # time.sleep(3)
    # pyautogui.press('enter')   # Isso talvez confirme a pasta atual
    # time.sleep(3)
    # pyautogui.write(filename)
    # pyautogui.press('enter')
    # Vamos manter essa sequência, mas ajustando para o caso de já estar na janela de salvamento.
    time.sleep(3)  # Aguarda a janela de salvamento aparecer
    pyautogui.press('enter')  # Talvez confirme a pasta padrão
    time.sleep(3)  # Aguarda a janela de salvamento aparecer (após confirmar pasta)
    print("Digitando o nome do arquivo...")
    pyautogui.write(filename)  # Escreve o nome do arquivo
    time.sleep(0.5)
    print("Pressionando Enter para salvar...")
    pyautogui.press('enter')

# Inicia thread que interagirá com a janela de salvamento
filename = f"{processo_numero}.pdf"
target_file = os.path.join(DOWNLOAD_PATH, f"{processo_numero}.pdf")
if os.path.exists(target_file):
    os.remove(target_file)
del target_file

dialog_thread = threading.Thread(target=handle_save_dialog, args=(filename,))
dialog_thread.start()

# Clica no botão via JavaScript (rápido, não bloqueia)
print("Clicando no botão 'Imprimir' via JavaScript...")
driver.execute_script("arguments[0].click();", imprimir_btn)
print("Clique JavaScript executado.")

# ------------------------------------------------------------
# 10. Monitorar download na pasta de Downloads do Windows
# ------------------------------------------------------------
def wait_for_download(path, timeout=TIMEOUT_DOWNLOAD):
    seconds = 0
    while seconds < timeout:
        files = [f for f in os.listdir(path) if f.lower().endswith('.pdf')]
        if files:
            # Pega o arquivo mais recente (por data de criação)
            latest = max([os.path.join(path, f) for f in files], key=os.path.getctime)
            return latest
        time.sleep(1)
        seconds += 1
    return None

print("Aguardando download do PDF na pasta Downloads...")
pdf_file = wait_for_download(DOWNLOAD_PATH)

if pdf_file:
    # Move para a pasta de destino com o nome correto
    new_name = os.path.join(PDF_SAVE_PATH, f"{processo_numero}.pdf")
    if os.path.exists(new_name):
        os.remove(new_name)
    os.rename(pdf_file, new_name)
    print(f"✅ PDF salvo como: {new_name}")
else:
    print("❌ Nenhum PDF baixado dentro do tempo limite.")

# ------------------------------------------------------------
# 11. Finalizar
# ------------------------------------------------------------
# driver.quit()