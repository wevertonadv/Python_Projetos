# Link: https://www.youtube.com/watch?v=L2dOYOhhDtg
import pandas as pd

#Para ler o nome de uma aba especifico sempre quandor for abrir um arquivo e tiver barra joga o r na frente para ler
tabela = pd.read_excel(r"c:\Users\joaol\Downloads\Vendas - Dez.xlsx", sheet_name='nome da aba')

#Para somar uma tabela usamos o nome da tabela e  o método .sum()
faturamento = tabela["Valor Final"].sum()

#Para contar uma tabela usamos o nome da tabela e o método .count()
faturamento = tabela["Valor Final"].count()

#Para calcular a média de uma uma tabela usamos o nome da tabela e o método .mean()
faturamento = tabela["Valor Final"].mean()


#para calcular a quantidade itens da coluna valor finale mostrar a quantidade com o print
faturamento = tabela["Valor Final"].sum()
print(faturamento)
quantidade = tabela["Valor Final"].sum()
print(quantidade)


#Explicação
#1. Para selecionar a coluna o nome da coluna usamos o colchete [] 
