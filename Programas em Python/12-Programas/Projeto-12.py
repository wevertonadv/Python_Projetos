#SEPARANDO DIGITOS DE UM NÚMERO DE 0 ATÉ 9999
#DESAFIO 12
numero = int(input("Digite um Número: "))
u = numero // 1   % 10
d = numero // 10   % 10
c = numero // 100  % 10
m = numero // 1000 % 10
unidade = print("A parte da unidade do número {} é:{}".format(numero,u))
denzena = print("A parte da denzena do número {} é:{}".format(numero,d))
centena = print("A parte da centena do número {} é:{}".format(numero,c))
milhar=print("A parte milhar do número {} é:{}".format(numero,m))
