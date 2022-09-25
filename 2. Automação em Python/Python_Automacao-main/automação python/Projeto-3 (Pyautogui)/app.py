###python
##from mouseinfo import mouseInfo
#mouseInfo()

# Abri a ferramenta
# Preencher o login
# Preencher a senha
# Clicar em fazer login

import pyautogui
pyautogui.alert("INICIAR")
#Abri a pasta
pyautogui.click(190,146,duration=0.4)
pyautogui.click()
#Abri a planilha
pyautogui.click(271,427,duration=0.4)
pyautogui.click()
# Preencher o login
pyautogui.click(513,241,duration=2)
pyautogui.write('weverton')
# Preencher a senha
pyautogui.click(526,286,duration=0.2)
pyautogui.write('123456')
# Clicar em fazer login
pyautogui.click(458,393,duration=0.2)

pyautogui.alert("Fim")