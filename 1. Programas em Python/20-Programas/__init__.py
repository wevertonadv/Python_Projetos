#Exercício Python #014 - Conversor de Temperaturas

##Escreva um programa que converta uma temperatura digitada em Celsius  para Fahrenheit

celsius = float(input('Digite a temperatura em °Celsius: '))
fahrenheit = ((9 * celsius) / 5) + 32
print('A temperatura de {}°Celsius corresponde a {}°Fahrenheit'.format(celsius,fahrenheit))