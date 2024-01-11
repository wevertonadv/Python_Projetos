#Crie uma função chamada par_ou_impar que recebe um número como argumento e retorna uma mensagem indicando se é par ou ímpar.


def par_ou_impar(numero):
    if numero % 2 == 0:
         return  "numero é par"
    else:
        print("O Número é impar")



resultado = par_ou_impar(4)
print(resultado)
