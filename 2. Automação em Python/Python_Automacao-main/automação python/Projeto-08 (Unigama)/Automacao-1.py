#Automação 01 Fazendo uma pesquisa no google e fazendo uma traduação

import time
from xml.dom.minidom import Element
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()

driver.get("https://www.google.com/")
time.sleep(0)

elem = driver.find_element(By.NAME,'q')
elem.clear()
elem.send_keys("Tradutor")
elem.send_keys(Keys.RETURN)
time.sleep(0)

elem = driver.find_element(By.ID, 'tw-source-text-ta')
elem.clear()
elem.send_keys("I love you")
elem.send_keys(Keys.RETURN)

assert "No results found." not in driver.page_source
