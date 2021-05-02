#CALCULANDO O AUMENTO DO SÁLÁRIO
# DESAFIO 3
print('DESAFIO 03')
salario = float(input('Digite seu saário atual R$: '))
porcentagem = int(input('Digite a porcentagem de aumento: '))
aumento = salario + (salario * porcentagem) / 100
acrescimo = aumento - salario
print('O valor do salário é de {:.2f} Reais e o aumento foi de {:.0f}% contabilizando {:.2f} Reais \n '
      'Tendo um aumento de {:.2f} Reais. ' .format(salario, porcentagem, aumento, acrescimo))