import time
#para esperar o elemento
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import TimeoutException 
from selenium.common.exceptions import NoSuchElementException



from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
driver.get("https://ead.unigama.com/my/")
assert "Unigama" in driver.title

#maximizando a tela do windows
driver.maximize_window()

driver.get('https://ead.unigama.com/course/view.php?id=1080')

#dando click no botão ativar edição

time.sleep(2)
elem = driver.find_element(By.CSS_SELECTOR,'.btn.btn-primary')

elem.click()

# Dando click no botão adicionar uma atividade
elem = driver.find_element(By.CSS_SELECTOR, '.section-modchooser-text')
elem.click()

# Dando click no botão adicionar uma atividade
elem = driver.find_element(By.CSS_SELECTOR, '.section-modchooser-text')
elem.click()
time.sleep(1)

elem = driver.find_element(By. XPATH, '/html/body/div[14]/div[2]/div/div/div[2]/div/div/div[1]/div/div[2]/div[2]/div[2]/div/div[9]/div/a/div[1]/img').click()

#Escrevendo Biblioteca Virtual
driver.find_element(By. ID, 'id_name').send_keys('Biblioteca A')

#Colocando o link
driver.find_element(By. ID, 'id_toolurl').send_keys('https://bibliotecaa.grupoa.com.br/lti/launch.php')

driver.find_element(By. CSS_SELECTOR,'.btn.btn-primary').click()

time.sleep(1)


# Apagar a outra biblioteca
driver.find_element(By. XPATH,'/html/body/div[6]/div[2]/div/div/section/div/div/div/div/ul/li[1]/div[3]/ul/li[6]/div/div/div[2]/div[2]/div/div/div[1]/a').click()

driver.find_element(By. XPATH, '/html/body/div[6]/div[2]/div/div/section/div/div/div/div/ul/li[1]/div[3]/ul/li[6]/div/div/div[2]/div[2]/div/div/div[1]/div/a[7]').click()
time.sleep(1)

#Apertando sim
driver.find_element(By. CSS_SELECTOR, '.modal-footer .btn.btn-primary').click()