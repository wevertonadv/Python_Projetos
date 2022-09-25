#!/usr/bin/env python
# coding: utf-8

# # Objetivo: Enviar mensagem para várias pessoas ou grupos automaticamente

# ### Cuidados!
# 
# 1. Whatsapp não gosta de nenhum tipo de automação
# 2. Isso pode dar merda, já to avisando
# 3. Isso não é o uso da API oficial do Whatsapp, o próprio whatsapp tem uma API oficial. Se o seu objetivo é fazer envio em massa ou criar aqueles robozinhos que respondem automaticamente no whatsapp, então use a API oficial
# 4. Meu objetivo é 100% educacional

# ### Dito isso, bora automatizar o envio de mensagens no Whatsapp
# 
# - Vamos usar o Selenium (vídeo da configuração na descrição)
# - Temos 1 ferramenta muito boa alternativas:
#     - Usar o wa.me (mais fácil, mais seguro, mas mais demorado)

# In[17]:


import pandas as pd

contatos_df = pd.read_excel("Enviar.xlsx")
display(contatos_df)


# In[20]:


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import urllib

navegador = webdriver.Chrome()
navegador.get("https://web.whatsapp.com/")

while len(navegador.find_elements_by_id("side")) < 1:
    time.sleep(1)

# já estamos com o login feito no whatsapp web
for i, mensagem in enumerate(contatos_df['Mensagem']):
    pessoa = contatos_df.loc[i, "Pessoa"]
    numero = contatos_df.loc[i, "Número"]
    texto = urllib.parse.quote(f"Oi {pessoa}! {mensagem}")
    link = f"https://web.whatsapp.com/send?phone={numero}&text={texto}"
    navegador.get(link)
    while len(navegador.find_elements_by_id("side")) < 1:
        time.sleep(1)
    navegador.find_element_by_xpath('//*[@id="main"]/footer/div[1]/div[2]/div/div[2]').send_keys(Keys.ENTER)
    time.sleep(10)
        
    


# In[ ]:




