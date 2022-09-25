#SORTEANDO 5 NOMES ALEATÓRIOS
#DESAFIO 11
from random import choice
print('|Sorteando  Nomes|')
nome1 = input("Participante 01: ")
nome2 = input("Participante 02: ")
nome3 = input("Participante 03: ")
nome4 = input("Participante 04: ")
nome5 = input("Participante 05: ")
participantes = [nome1, nome2, nome3, nome4, nome5]
escolhodo = choice(participantes)
print("Parabéns {} você ganhou".format(escolhodo))
