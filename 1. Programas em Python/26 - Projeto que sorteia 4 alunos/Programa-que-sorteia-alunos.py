import random

pessoa1 = input('Digite o noem do aluno 1:  ')
pessoa2 = input('Digite o noem do aluno 2:  ')
pessoa3 = input('Digite o noem do aluno 3:  ')
pessoa4 = input('Digite o noem do aluno 4:  ')
lista = [pessoa1, pessoa2, pessoa3, pessoa4]
sorteio = random.choice(lista)
print(' aluno sorteado foi', sorteio)
print(lista)