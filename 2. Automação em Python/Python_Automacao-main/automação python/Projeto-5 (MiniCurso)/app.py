#https://www.youtube.com/watch?v=8AMNaVt0z_M&t=0s

from selenium import webdriver

navegador = webdriver.Chrome(executable_path=r'./chromedriver.exe')

#Passo 1 /.get serve para acessar alguma coisa 
navegador.get('https://pages.hashtagtreinamentos.com/inscricao-minicurso-python-automacao-org?origemurl=hashtag_yt_org_minipython_videoselenium')

# Passo 2: Preencher nome, preencher e-mail /.find_element() serve para localizar qualquer elemento
navegador.find_element('xpath', 
'//*[@id="section-10356508"]/section/div[2]/div/div[2]/form/div[1]/div/div[1]/div/input').send_keys('lira')

navegador.find_element('xpath',
'//*[@id="section-10356508"]/section/div[2]/div/div[2]/form/div[1]/div/div[2]/div/input').send_keys('pythonimpressionador@gmail.com')

# Passo 3: Clicar no botão para enviar o formulário
navegador.find_element('xpath', 
'//*[@id="section-10356508"]/section/div[2]/div/div[2]/form/button').click()
