import time
from selenium.common.exceptions import NoSuchElementException
from lib2to3.pgen2.token import NAME
from selenium import webdriver
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.common.by import By
import pandas as pd

driver = webdriver.Chrome()
driver.get('https://ead.unigama.com/login/index.php')
assert "Unigama: Acesso ao site" in driver.title

elem = driver.find_element(By.NAME,'username')
elem.clear()
elem.send_keys('weverton_machado')
#Preenchendo a senha
elem = driver.find_element(By.NAME,'password')
elem.clear()
elem.send_keys('weverton123@')
#Dando enter
elem.send_keys(Keys.RETURN)