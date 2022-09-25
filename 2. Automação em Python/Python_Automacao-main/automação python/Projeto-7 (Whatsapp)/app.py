import keyword
from IPython.display import display
import pandas as pd

contatos_df = pd.read_excel("Enviar.xlsx")
display(contatos_df)

#Import da biblioteca selenium, time 
from selenium import webdriver
#importação das teclas do teclado por explo: o enter
from selenium.webdriver.common.keys import Keys
import time
import urllib
from selenium.webdriver.common.by import By




#PASSO 0: ABRIR O NAVEGADOR
navegador = webdriver.Chrome()
navegador.get('https://web.whatsapp.com/')


#PASSO 1: esperaR aparecer o elemento que tem id de "pane-side" Tradução: enquanto o tamano dessa lista for menor que 1 espera 1s
while len (navegador.find_element(By .ID,'pane-side')) <1:
    time.sleep(1)


#PASSO 2: loop para envio de mensagens para os contatos
for i, mensagem in enumerate(contatos_df['Mensagem']):
    pessoa = contatos_df.loc[i,'Pessoa']
    numero = contatos_df.loc[i, 'Número']
    texto = urllib.parse.quote( f" Oi {pessoa}! {mensagem}")
    link = f"https://web.whatsapp.com/send?phone={numero}&text={texto}"
    navegador.get(link)
    
    #Esperando o wpp carregar
    while len (navegador.find_element('xpath', '//*[@id="side"]/div[1]/div/button/div/span')) <1:
        time.sleep(1)

    #dando enter na mensagem    
    navegador.find_element('xpath','//*[@id="main"]/footer/div[1]/div/span[2]/div/div[2]/div[1]/div/div/p').send_keys(Keys.ENTER)
    time.sleep(10)
        





