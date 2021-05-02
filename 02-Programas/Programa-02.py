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