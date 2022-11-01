import time
from selenium.common.exceptions import NoSuchElementException
from lib2to3.pgen2.token import NAME
from selenium import webdriver
from selenium.webdriver.common.keys import Keys 

import pandas as pd

driver = webdriver.Chrome()
driver.get('https://ead.unigama.com/login/index.php')
assert "Unigama: Acesso ao site" in driver.title

#login
elem = driver.find_element(By.NAME,'username')
elem.clear()
elem.send_keys('weverton_machado')
#Preenchendo a senha
elem = driver.find_element(By.NAME,'password')
elem.clear()
elem.send_keys('weverton123@')
#Dando enter
elem.send_keys(Keys.RETURN)


#acessando a disciplina
driver.get('https://ead.unigama.com/user/index.php?id=1068')


#clicando no botão inscrever usuário
driver.maximize_window()
driver.find_element(By.XPATH,'//*[@id="enrolusersbutton-2"]/div/input[1]').click()
time.sleep(2)

#clicando na busca e inserindo o nome
elem = driver.find_element(By.CSS_SELECTOR,'.d-md-inline-block.mr-md-2.position-relative .form-control')
elem.send_keys('teste.ava@gmail.com')
time.sleep(5)
elem.send_keys(Keys.RETURN)


#dando click no botão inscrever usuários
elem = driver.find_element(By.CSS_SELECTOR,'.modal-content .btn-primary').click()

import pandas as pd
tabela = pd.read_excel('alunos.xlsx')

for i, email in enumerate(tabela["E-mail"]):
    #clicando no botão inscrever usuário
    driver.maximize_window()
    driver.find_element(By.XPATH,'//*[@id="enrolusersbutton-2"]/div/input[1]').click()
    time.sleep(2)

    #clicando na busca e inserindo o nome
    elem = driver.find_element(By.CSS_SELECTOR,'.d-md-inline-block.mr-md-2.position-relative .form-control')
    elem.send_keys(email)
    time.sleep(5)
    elem.send_keys(Keys.RETURN)


    #dando click no botão inscrever usuários
    elem = driver.find_element(By.CSS_SELECTOR,'.modal-content .btn-primary').click()


