#CALCULANDO O PREÇO DE UM CARRO ALUGADO
#DESAFIO 05
print('DESAFIO 05')
km_pecorrido = float(input('Digite a quantidade de km pecorrido:' ))
dias = int(input('Digite quantos dias você ficou com o carro: '))
totaldias = dias * 60
totalkm = km_pecorrido * 0.15
tudo2 = (totalkm + totaldias)
print('Total a pagar {:.2f} R$'.format(tudo2))
