#Extra quando não precisamos usar a variavel depois
nu1 = int(input('Digite Um valor: '))
nu2 = int(input('Digite outro valor: '))
s = nu1 + nu2
m = nu1 * nu2 
d = nu1 / nu2
di = nu1 // nu2
p = nu1 ** nu2
print('A soma é {}, \n a multiplicação é {}, \n a divisão é {:.2f}, \n a divisão inteira é {}'.format(s,m,d,di), end='')
print('A potenciação e {}'.format(p))

n1 = 4**3
n2 = pow(4,3)
print(n1)
print(n2)

# Raiz quadrada
n3 = 81**(1/2)
print(n3)

n4 = 127**(1/3)
print(n4)

#Concatenando
n5 = 'oi' + 'olá'
print(n5)

#Multiplicando
n6 = "==" * 50
print(n6)

#Extra
nome = input('Digite seu nome ')
print('Prazer em te conhecer {:20}!'.format(nome))

nome = input('Digite seu nome ')
print('Prazer em te conhecer {:=w^20}!'.format(nome))

nome = input('Digite seu nome ')
print('Prazer em te conhecer {:>20}!'.format(nome))

nome = input('Digite seu nome ')
print('Prazer em te conhecer {:<20}!'.format(nome))

nome = input('Digite seu nome ')
print('Prazer em te conhecer {:^20}!'.format(nome))

