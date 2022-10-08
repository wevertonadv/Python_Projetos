import pyautogui
from time import sleep 

#Automatizar sistemas com python

pyautogui.alert("iniciar programa")

#0 cadastrar o usuario
pyautogui.click(715,438,duration=0.5)

#digitar o usuario
pyautogui.click(731,387,duration=0.10)
pyautogui.write('aa')
#digitar a senha
pyautogui.click(755,415,duration=0.5)
pyautogui.write('aa')
#clicar no registar
pyautogui.click(641,442,duration=0.5)

#1 Clicar e ditar meu usuario
pyautogui.click(683,386,duration=0.1)
pyautogui.write("aa")
#2 Clicar e digitar minha senha
pyautogui.click(683,410,duration=0.1)
pyautogui.write("aa")
#3 Clicar em Eentrar
pyautogui.click(598,438)
#4 Extratir cada produto
with open('produtos.txt','r') as arquivos: 
    for linha in arquivos:
        produto = linha.split(',')[0]
        quantidade = linha.split(',')[1]
        preco = linha.split(',')[2]
        #1 clicar e digitar produto
        pyautogui.click(446,374)
        pyautogui.write(produto)
        #2 Clciar e digitar quantidade
        pyautogui.click(448,399)
        pyautogui.write(quantidade)
        #3 Clicar e digitar preço
        pyautogui.click(411,423,duration=0.2)
        pyautogui.write(preco)
        #4 Clciar em registrar
        pyautogui.click(321,580)
        sleep(0)

pyautogui.alert("FIM")





#----------------------------------- Rodapé -----------------------------------#

# Cadastramento de produtos em massa para testar aplicação entra na pasta programa e abre o arquivo app.exe;

#link https://www.youtube.com/watch?v=pNBjC32nisg&t=852s