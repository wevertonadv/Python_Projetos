#CONVERSÃO DE METROS PARA MILÍMETROS
#DESAFIO 1
print('DESAFIO 01')
medida = float(input('escolha uma distância: '))
mm = medida * 1000
print(' A medida de {:.0f}m Corresponde a {:.0f}mm'.format(medida,mm))

#CALCULANDO O TOTAL DE SEGUNDOS
#DESAFIO 2
print('DESAFIO 02')
a = int(input("digite a quantidade de dias!"))
b = int(input("digite a quantidade de horas!"))
c= int(input("digite a quantidade de minutos! "))
d = int(input("digite a quantodade de segundos! "))

ds = a * 86400
hs = b * 3600
ms = c * 60

print("a quantidade total de tempo  em segundo é, ",ds + hs+ ms + d)

#CALCULANDO O AUMENTO DO SÁLÁRIO
# DESAFIO 3
print('DESAFIO 03')
salario = float(input('Digite seu saário atual R$: '))
porcentagem = int(input('Digite a porcentagem de aumento: '))
aumento = salario + (salario * porcentagem) / 100
acrescimo = aumento - salario
print('O valor do salário é de {:.2f} Reais e o aumento foi de {:.0f}% contabilizando {:.2f} Reais \n '
      'Tendo um aumento de {:.2f} Reais. ' .format(salario, porcentagem, aumento, acrescimo))

#CALCULANDO O TEMPO DE UMA VIAGEM
#DESAFIO 04
print('DESAFIO 04')
distancia = float(input("distância percorrida em KM: "))
tempo = float (input("Digite a velocidade média em km/h: "))
velocidade = distancia / tempo
print(" o tempo estimado é de {:.2f} horas".format(velocidade))

#CALCULANDO O PREÇO DE UM CARRO ALUGADO
#DESAFIO 05
print('DESAFIO 05')
km_pecorrido = float(input('Digite a quantidade de km pecorrido:' ))
dias = int(input('Digite quantos dias você ficou com o carro: '))
totaldias = dias * 60
totalkm = km_pecorrido * 0.15
tudo2 = (totalkm + totaldias)
print('Total a pagar {:.2f} R$'.format(tudo2))

#SOMA/SUBTRAÇÃO/DIVISÃO/MULTIPLICAÇÃO DE UM NÚMERO INTEIRO
#DESAFIO 06
print('DESAFIO 06')
n1 = int(input('Digite um número: '))
n2 = int(input('Digite outro número: '))
soma= n1 + n2
subtracao = n1 - n2
divisao = n1 / n2
multiplicacao = n1 * n2
print('A soma é {} a Subtração é {} a divisão é {} e a multiplicação é {}'.format
      (soma,subtracao,divisao,multiplicacao))


#TABUADA DE UM NÚMERO
#DESAFIIO 07
print('DESAFIO 07')
tabu= int(input('Digite um número para ver sua tabuada: '))
print ('__' *8)
print('{} x {:2} = {}'.format(tabu, 1, tabu*1))
print('{} x {:2} = {}'.format(tabu, 2, tabu*2))
print('{} x {:2} = {}'.format(tabu, 3, tabu*3))
print('{} x {:2} = {}'.format(tabu, 4, tabu*4))
print('{} x {:2} = {}'.format(tabu, 5, tabu*5))
print('{} x {:2} = {}'.format(tabu, 6, tabu*6))
print('{} x {:2} = {}'.format(tabu, 7, tabu*7))
print('{} x {:2} = {}'.format(tabu, 8, tabu*8))
print('{} x {:2} = {}'.format(tabu, 9, tabu*9))
print('{} x {} = {}'  .format(tabu, 10, tabu*10))
print ('__' *8)


#FARENHEIT PARA CELSIUS
#DESAFIO 08
print('DESAFIO 08')
farenheit = float(input('Digite a temperatura em farenheit: '))
celsius =  5/9 * (farenheit - 32 )
print('A sua temperatura em farenheit  é {}° para celsius é {:.2f}°C'.format(farenheit,celsius))


#CALCULAR E INFORMAR A PORCENTAGEM DOS ITENS ABAIXO
#DESAFIO 09
print('DESAFIO 09')
n1 = int(input("Quantidade de alunos do sexo masculino: "))
n2 = int(input("Quantidade de alunos do sexo feminino : "))
alunosaprovado = int(input('Quantidade de alunos aprovados: '))
total = (n1 + n2 )
reprovados = total-alunosaprovado
p_mulher = (n2 * 100) /total
p_homem = (n1 * 100 ) / total
p_reprovado =  (reprovados*100) / total
n1p = (n1 / 100)
print('O total de alunos é {}'.format(total))
print('Sendo {:.2f}% do sexo masculino e {:.2f}% do sexo femenino dentre eles {:.2f}% foram reprovados'
      .format(p_homem,p_mulher,p_reprovado))


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
