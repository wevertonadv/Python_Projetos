
import time
#Import da web driver
from selenium import webdriver
# import do by
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Import por exemplo das teclas
from selenium.webdriver.common.keys import Keys

import pandas as pd
import openpyxl

navegador =webdriver.Chrome()
navegador.get('https://ead.unigama.com/course/modedit.php?update=11224')


usuario = navegador.find_element(By. ID,'username').send_keys('')
senha = navegador.find_element(By. ID,'password').send_keys('')
navegador.find_element(By. ID, 'loginbtn').click()

navegador.maximize_window()


disciplina = navegador.get('https://ead.unigama.com/course/modedit.php?update=11224')

navegador.find_element(By.XPATH,'/html/body/div[3]/div[2]/div/div/section/div/div/div/form/fieldset[2]/legend/a').click()

#Alterando o dia da prova da n2
dia = 8
dia = navegador.find_element(By.ID, 'id_timeclose_day').send_keys(dia)
time.sleep(1)

#Salvando
navegador.find_element(By. XPATH, '/html/body/div[3]/div[2]/div/div/section/div/div/div/form/div[3]/div[2]/fieldset/div/div[1]/span/input').click()

import pandas as pd
exceldata = pd.read_excel("file.xlsx",sheet_name = "sheet1")
for idx in exceldata.index:
   print(exceldata['links'][idx])

tabela = pd.read_excel('DatasN2.xlsx')

for i, modulo in enumerate(tabela['MODULO']):
    navegador.find_element(By.XPATH,'/html/body/div[3]/div[2]/div/div/section/div/div/div/form/fieldset[2]/legend/a').click()

    #Alterando o dia da prova da n2
    dia = 8
    dia = navegador.find_element(By.ID, 'id_timeclose_day').send_keys(dia)
    time.sleep(1)

    #Salvando
    navegador.find_element(By. XPATH, '/html/body/div[3]/div[2]/div/div/section/div/div/div/form/div[3]/div[2]/fieldset/div/div[1]/span/input').click()




















#https://stackoverflow.com/questions/57877458/how-to-use-python-to-retrieve-an-url-from-an-excel-column-and-then-open-it-with