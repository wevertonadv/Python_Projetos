#Desafio 16
# Crie um programa que leia um número Real qualquer pelo teclado e mostre na tela sua porção inteira


# primeiramente pede o número a pessoa e importa a bilioteca math e importa o módulo trunc que corta a parte a real
# do número. O from math import trunc, só importa o módulo trunc da biblioteca math


#jeito1
from math import trunc
num = float(input('Digite um número: '))
print('O número digitado foi {} e a sua porção inteira é {}'.format(num, trunc(num)))


#jeito1
num1 = float(input('Digite um valor: '))
print('O valor digitado foi {} e a sua porção inteira é {}'.format(num, int(num))) #colocando o int na frente só vou usar a parte inteira


