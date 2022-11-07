from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import pandas as pd

driver = webdriver.Chrome()
driver.get("https://ead.unigama.com/my/")

# Acessando a disciplina        
driver.get('https://ead.unigama.com/course/view.php?id=1172')

# Entrando nas configurações da disciplina
elem  =  driver.find_element(By. XPATH, '/html/body/div[3]/div[2]/header/div/div/div/div[1]/div[2]/div/div/div/div/a/i')

elem.click()

# Clicando no botão editar configurações
elem = driver.find_element(By. XPATH, '/html/body/div[3]/div[2]/header/div/div/div/div[1]/div[2]/div/div/div/div/div/div[1]/a')
elem.click()

# Clicando em aparencia 
elem = driver.find_element(By.PARTIAL_LINK_TEXT, "Aparência")
elem.click()

# Clicando em mostrar livro de notas aos estudantes
elem = driver.find_element(By. ID, 'id_showgrades')
elem.click()

# Clicando em Sim
elem = driver.find_element(By. XPATH, '/html/body/div[4]/div[2]/div/div/section/div/div/div/form/fieldset[4]/div/div[3]/div[2]/select/option[2]')
elem.click()

#clicando em salvar mudanças
driver.find_element(By. ID, 'id_saveanddisplay').click()
time.sleep(1)


# Entrando nas configurações da disciplina pelo full
elem  =  driver.find_element(By. XPATH, '/html/body/div[3]/div[2]/header/div/div/div/div[1]/div[2]/div/div/div/div/a/i')
elem.click()

# Clicando em livro de notas pelo full
elem  =  driver.find_element(By. XPATH, '/html/body/div[3]/div[2]/header/div/div/div/div[1]/div[2]/div/div/div/div/div/div[4]/a')
elem.click()

# Ocutando o somans 
driver.find_element(By. ID, 'action-menu-toggle-7').click()

#Clicando em Ocultar
driver.find_element(By. XPATH, '/html/body/div[3]/div[2]/div/div/section/div/div/div/div[2]/form/div/table/tbody/tr[7]/td[4]/div/div/div/div/div/a[3]').click()

# Clicando em Total do curso
driver.find_element(By. ID, 'action-menu-toggle-1').click()

# Clicando em editar texto
driver.find_element(By. PARTIAL_LINK_TEXT, 'Editar cálculo').click()

# Apagando o calculo
driver.find_element(By. ID, 'id_calculation').clear()

# Salvando mudanças
elem = driver.find_element(By. CSS_SELECTOR, '.btn.btn-primary').click()
time.sleep(1)