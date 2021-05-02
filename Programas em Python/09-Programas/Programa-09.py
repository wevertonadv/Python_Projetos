#CALCULAR E INFORMAR A PORCENTAGEM DOS ITENS ABAIXO
#DESAFIO 09
print('DESAFIO 09')
n1 = int(input("Quantidade de alunos do sexo masculino: "))
n2 = int(input("Quantidade de alunos do sexo feminino : "))
alunosaprovado = int(input('Quantidade de alunos aprovados: '))
total = (n1 + n2 )
reprovados = total-alunosaprovado
p_mulher = (n2 * 100) /total
p_homem = (n1 * 100 ) / total
p_reprovado =  (reprovados*100) / total
n1p = (n1 / 100)
print('O total de alunos é {}'.format(total))
print('Sendo {:.2f}% do sexo masculino e {:.2f}% do sexo femenino dentre eles {:.2f}% foram reprovados'
      .format(p_homem,p_mulher,p_reprovado))