#Faça um algoritmo que leia o preço de um produto e mostre seu novo preço, com 5% de desconto.

preco = float(input("Digite o preço do produto: "))
novo =  preco -(preco * 5 ) / 100
print("o preço do produto é {} com um desconto de 5% fica em {}".format(preco,novo))
