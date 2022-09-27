
#Import da web driver
from selenium import webdriver
# Import por exemplo das teclas
from selenium.webdriver.common.keys import Keys


# import do by
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


navegador = webdriver.Chrome()
navegador.get('https://ead.unigama.com/course/view.php?id=1068')


navegador.find_element(By. ID,'username').send_keys('')
navegador.find_element(By. ID,'password').send_keys('')
navegador.find_element(By. ID, 'loginbtn').click()

## Nome da disciplina
navegador.get('https://ead.unigama.com/course/view.php?id=1068')

# acessando a prova
navegador.find_element(By. XPATH, '//*[@id="module-17333"]/div/div/div[2]/div[1]/a/span').click()

#clicando na engrenagem
navegador.find_element(By. ID, 'action-menu-toggle-3').click()

#Clicando na opção sobreposições de usuário
navegador.find_element(By. XPATH,'/html/body/div[2]/div[3]/div/div/div/div/div/div/div/div/div/div[3]/a').click()

navegador.find_element(By. CLASS_NAME, 'btn-secondary').click()

## Email do aluno
navegador.find_element(By. CSS_SELECTOR, 'input.form-control').send_keys('teste.ava@gmail.com').send_keys(Keys.ENTER)
