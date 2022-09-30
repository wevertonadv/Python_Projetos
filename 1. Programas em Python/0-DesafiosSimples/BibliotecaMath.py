#Raiz quadrada de um numero
import math 
num = int(input('Digite um número: '))
raiz = math.sqrt(num)
potencia = math.pow(num,2)
print('A raiz quadra de {} é {:.0f}'.format(num, raiz))
print('A potenciação do n° {} é {}'.format(num, potencia))