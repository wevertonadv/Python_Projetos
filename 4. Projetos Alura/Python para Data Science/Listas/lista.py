#criando uma lista
lista = ['Weverton Mello', 10, 9.5, 2.5, "Ivonete Sarsedo", True]
print(lista)


#quantidade de elementos dentro da lista
print(len(lista))


# Partição da lista / dividir a lista começar no meu nome e terminar no da minha mãe 
print(lista[0:5])


#Começa no quarto elemento até o final
print(lista[4:])

#Começa no quarto elemento até o final
print(lista[:4])

#adicionado elementos no final da lista
lista.append('SetimoElemento')
print(lista)

#adicionado elementos de uma várial em uma lista
lista.extend('SetimoElemento''OitavoElemento')
print(lista)
