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