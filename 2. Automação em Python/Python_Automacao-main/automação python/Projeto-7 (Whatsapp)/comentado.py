# A biblioteca pandas ler o arquivo em excel e joga dentro do arquivo em python. Ex: eu estou armazendando as informações da planilha dentro do contatos_df
import pandas as pd

contatos_df = pd.read_excel("Enviar.xlsx")

#Import da biblioteca selenium, time 
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time




#PASSO 0: ABRIR O NAVEGADOR
navegador = webdriver.Chrome()
navegador.get('https://web.whatsapp.com/')


#PASSO 1: esperaR aparecer o elemento que tem id de "pane-side" Tradução: enquanto o tamano dessa lista for menor que 1 espera 1s
while len (navegador.find_element(By.ID, 'pane-side')) <1:
    time.sleep(1)


#PASSO 2: AGORA JÁ ESTAMOS COM O LOGIN DO WPP ABERTO
for i, mensagem in enumerate(contatos_df['Mensagem']):
    pessoa = contatos_df.loc[i,  'Pessoa']
    numero = contatos_df.loc[i, 'Número']




