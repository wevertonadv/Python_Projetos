
# para esperar o elemento
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import TimeoutException

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
driver.get("https://ead.unigama.com/course/view.php?id=1101")
assert "Unigama" in driver.title

# maximizando a tela do windows
driver.maximize_window()


#acessando as unidades
elementos = driver.find_elements(By.CSS_SELECTOR,'#section-1 ul.section.img-text a.aalink')[:-2]

links = []

for elemento in elementos: 
   links.append(elemento.get_attribute('href'))

erro = []
sucesso = []

if len(links) > 0:
   for link in links:
      driver.get(link)
      try:
         presente = EC.presence_of_element_located((By.CSS_SELECTOR,'.message-error-wrapper'))
         WebDriverWait(driver,8).until(presente)
         erro.append(link)
      except TimeoutException:
         sucesso.append(link)

print('Lista de sucesso \n', sucesso)
print('Lista de erro \n', erro)


# se der erro vai aparecer aqui
