#JOGO DA ADIVINHAÇÃO
#DESAFIO 10
print('DESAFIO10')
from random import randint
from time import sleep

computador = randint(0, 5)  # Faz o computador sortear um número 0 a 5
print('-=- ' * 20 )
print('Pense em um Número de 0 a 5 e tente adivinhar')
print('-=-' * 20 )
jogador = (int(input('Digite um Número de 0 a 5:')))
print('Carregando...')
sleep(3)
if computador == jogador:
    print('Parabéns Voce ganhou com o número {}'.format(jogador))
else:
    print('Você perdeu o número pensado foi {}'.format(computador))