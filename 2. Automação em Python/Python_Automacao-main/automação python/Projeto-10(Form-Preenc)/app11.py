import time
from xml.dom.minidom import Element
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd


#essa linha é a criação e a integração do navegador com o webdrvier
navegador = webdriver.Chrome()
navegador.get('https://docs.google.com/forms/d/e/1FAIpQLSda4p49eSWJ5BrD_xHJAtinulrONhJBoTLLESor8ML6ffHZuw/viewform?usp=sf_link')
 
navegador.maximize_window()
#Preencher CPF - Selecionar e escrever dentro dele
navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[1]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys('11122233344')

#Preencher email
navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys('emailaleatorio1@gmail.com')

#preencher descrição
navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[3]/div/div/div[2]/div/div[1]/div[2]/textarea').send_keys('Curso de Excel')

#Preencher valor
navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[4]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys('1500')

#Clicar no botao
navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[3]/div[1]/div[1]/div/span/span').click()


#lendo a base de dados dentro do python pd é o nome que escolhi
import pandas as pd
tabela = pd.read_excel('Emitir.xlsx')

for i, cpf in enumerate(tabela["CPF"]):
    email = tabela.loc[i,"Email"]
    descricao = tabela.loc[i,"Descrição"]
    valor = tabela.loc[i,"Valor"]

    navegador.get('https://docs.google.com/forms/d/e/1FAIpQLSda4p49eSWJ5BrD_xHJAtinulrONhJBoTLLESor8ML6ffHZuw/viewform?usp=sf_link')

    #Preencher CPF - Selecionar e escrever dentro dele
    navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[1]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys(cpf)

    #Preencher email
    navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys(email)

    #preencher descrição
    navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[3]/div/div/div[2]/div/div[1]/div[2]/textarea').send_keys(descricao)

    #Preencher valor
    navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[4]/div/div/div[2]/div/div[1]/div/div[1]/input').send_keys(str(valor))

    #Clicar no botao
    navegador.find_element(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[3]/div[1]/div[1]/div/span/span').click()

    




