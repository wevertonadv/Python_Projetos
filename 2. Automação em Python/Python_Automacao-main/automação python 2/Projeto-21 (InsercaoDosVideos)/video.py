import time
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

import pandas as pd

#abrindo um navegador e acessando a página da unigama
driver = webdriver.Chrome()
driver.get('https://ead.unigama.com/login/index.php')
assert "Unigama: Acesso ao site" in driver.title


