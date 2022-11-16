salario = float(input('Qual é seu salario? '))
aumento = salario

if salario <= 1250:
     novo = (salario * 15 / 100 ) + salario
else:
    novo = (salario * 10 / 100 )  + salario
print(f'seu salario é {salario} mais com aumento foi para {novo}')