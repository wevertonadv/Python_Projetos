#https://www.youtube.com/watch?v=pZ6EqsHskM8
# Acessando o outlok (automação 6)
import time
from selenium import webdriver

navegador = webdriver.Chrome()

#acessando o navegador
navegador.get('https://login.live.com/')

#adicionando o email  
navegador.find_element('xpath','//*[@id="i0116"]').send_keys('coloque seu email')
time.sleep(1)

#clicnado no botão proximo
navegador.find_element('xpath','//*[@id="idSIButton9"]').click()
time.sleep(1)

#adicionando a senha
navegador.find_element('xpath','//*[@id="i0118"]').send_keys('coloque sua senha')
time.sleep(1)

#clicnado no botão sim
navegador.find_element('xpath','//*[@id="idSIButton9"]').click()
navegador.find_element('xpath','//*[@id="idSIButton9"]').click()


