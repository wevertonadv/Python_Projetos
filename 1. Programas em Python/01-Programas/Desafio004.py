
# o n é um objetto e o is é um método por isso que tem parenteses
n = input('Digite algo: ')
print(type(n))
print('',n.isalnum())

print('é alfabético?',n.isalpha())
print('tem letra ou número',n.isalnum())

print(n.isdecimal(),format('É um número decimal?'))

print(f'É um número? {n.isnumeric()}') #novo jeito do python 3.7 para substituir o format


print('São todas as letras maiusculas?',n.isupper())
print('São todas as letras minusculas?',n.islower())

print('Está capitalizada',n.istitle())
print('So tem espaços?',n.isspace())



