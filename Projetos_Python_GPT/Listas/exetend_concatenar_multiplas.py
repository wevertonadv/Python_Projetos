#Utilize o método extend() para concatenar várias listas em uma só.

# Criar listas
lista1 = [10, 20]
lista2 = [30, 40]
lista3 = [50, 60]

lista1.extend(lista2)
lista1.extend(lista3)

print("Lista estendida:", lista1)  # Saída: [1, 2, 3, 4, 5, 6]

