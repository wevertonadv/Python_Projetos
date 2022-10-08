import pyautogui
import time # biblioteca que fala com o cód parar 1s por exemplo...
 
pyautogui.alert("Não use nada do seu computador")
pyautogui.PAUSE = 1 #cód de todas as linhas

#ABRI O GOOGLE DRIVER
pyautogui.press('winleft')
pyautogui.write("chome")
pyautogui.press('enter')
pyautogui.moveTo(936,62)
pyautogui.click()
time.sleep(1) #Parar um segundo quando chegar no enter 
pyautogui.write("https://drive.google.com/drive/u/0/my-drive")
pyautogui.press('enter')
time.sleep(1)

#ENTRAR NA ÁREA DE TRABALHO
pyautogui.hotkey('winleft','d')

#CLIQUEI NO ARQUIVO DO BACKUP E ARRASTAR ELE
pyautogui.moveTo(336,43)
pyautogui.mouseDown()
pyautogui.moveTo(722, 626)

#ENQUANTO EU TO ARRASTANDO ELE EU VOU MUDAR PARA O GOOGLE DRIVE
pyautogui.hotkey('alt','tab')
time.sleep(1)

#LARGUEI O ARQUIVO NO GOOGLE 
pyautogui.mouseUp()

#Esperar 5 segundo para upload
time.sleep(5)
pyautogui.alert("O código acabou!")





#----------------------------------- Rodapé -----------------------------------#

# Automação que faz upload de um arquivo do seu computador para o google driver

#link 

# Bibliotecas
# Pyautogui: Para controlar o teclado e mouse
# Time:      Para colocar uma pausa durante a automação