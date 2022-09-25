#Automação 01 Fazendo uma pesquisa no google e fazendo uma traduação

import time
from xml.dom.minidom import Element
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()

driver.get("https://ead.unigama.com/login/index.php")
time.sleep(0)

#maximizando a tela do windows
driver.maximize_window()

#colocando usuario e senha
elem = driver.find_element(By.NAME,'username')
elem.clear()
elem.send_keys("weverton_machado")
elem = driver.find_element(By.NAME,'password')
elem.clear()
elem.send_keys("weverton123@")
elem.send_keys(Keys.RETURN)
time.sleep(1)




#clicando nas disciplinas
elem = driver.find_element(By.XPATH, '//*[@id="module-148756"]/div/div/div[2]/div/div/div/div/div[2]/div/a/img').click


elem.clear()
elem.send_keys("unigama")
elem.send_keys(Keys.RETURN)

assert "No results found." not in driver.page_source
