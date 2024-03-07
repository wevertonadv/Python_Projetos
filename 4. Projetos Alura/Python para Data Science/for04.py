#1) Escreva um programa que peça dois números inteiros e imprima todos os
# números inteiros entre eles.




a1 = int(input('Digite um número inteiro: '))
a2 = int(input('Digite outro número inteiro: '))

# Encontrar o menor e o maior número
menor_numero = min(a1, a2)
maior_numero = max(a1, a2)

# Imprimir todos os números inteiros entre eles
print(f"Números entre {menor_numero} e {maior_numero}:")
for i in range(menor_numero + 1, maior_numero):
    print(i)
    