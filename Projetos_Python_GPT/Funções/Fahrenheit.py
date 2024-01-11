# Crie uma função chamada celsius_para_fahrenheit que recebe uma temperatura em Celsius como argumento e a converte para Fahrenheit.


def celsius_para_fahrenheit(celsius):
    return (celsius * 9/5) + 32


temp_celsius = 13
temp_fahrenheit = celsius_para_fahrenheit(temp_celsius)
print(f"{temp_celsius}°C é igual a {temp_fahrenheit}°F")