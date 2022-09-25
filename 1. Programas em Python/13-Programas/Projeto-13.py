#Primeiro e ultimo nome de uma pessoa
#DESAFIO 13

digta = str(input("Digite seu nome completo: ")).strip()
nome2 = digta.split() #lista dividindo os nomes
print("Seu Primeiro nome é {}".format(nome2[0]))
print("Seu último nome é {}".format(nome2[len(nome2)-1]))
