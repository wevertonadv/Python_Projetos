# Faça um algoritmo que leia o salário de um funcionário e mostre seu
# novo salário, com 15% de aumento.

salario = float(input("Digite o salario: "))
aumento = salario + (salario * 15 / 100)
print("Seu Salario é {:.2f} com o aumento de 15% fica em {:.2f}".format(salario,aumento))