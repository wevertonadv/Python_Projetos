#Link https://www.youtube.com/watch?v=TknTtYa0yYI&list=PLGFzROSPU9oUESl0MosXC6VblJxMrwjOM&index=3
from datetime import datetime
from tkinter import * #Para a parte de front end
import tkinter
from  datetime import datetime

###### Cores usadas #######
cor1 = "#3d3d3d"  # preta
cor2 = "#fafcff"  # branca
cor3 = "#21c25c"  # verde
cor4 = "#eb463b"  # vermelha
cor5 = "#dedcdc"  # cinza
cor6 = "#3080f0"  # azul

#variaveis recebendo a cor do fundo e a cor do texto
fundo = cor1
texto = cor2

# Configuraçaõ da jenla criando a janela,sem titulo,não pode mudar o tamanho e a cor
janela=Tk()
janela.title("")
janela.geometry("440x180")
janela.resizable(width=FALSE, height=FALSE)
janela.configure(bg=cor1)

tempo=datetime.now() # estou pegando a hora da minha maquina e guardando dentro da variavel tempo

def relogio():
    hora = tempo.strftime("%H:%M:%S") #H hora M minutos S segundos
    print(hora)
    dia_semana = tempo.strftime("%A")
    print(dia_semana)
    dia=tempo.day
    print(dia)
    mes=tempo.strftime("%B") # B || b
    print(mes)
    ano = tempo.strftime("%Y")
    print(ano)

l1=Label(janela, text="10:50:22",)
janela.mainloop()

