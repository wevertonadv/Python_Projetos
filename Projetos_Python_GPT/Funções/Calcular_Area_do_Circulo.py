#Desenvolva uma função chamada area_circulo que calcula e retorna a área de um círculo com base no raio fornecido.


import math

def area_circulo(raio):
    return math.pi * raio ** 2


raio = 3
area = area_circulo(raio)
print("A área do círculo é:", area)
