# Faça um programa que leia a largura e a altura de uma parede em metros, calcule a sua área e a quantidade de tinta necessária para pintá-la, sabendo que cada litro de tinta pinta uma área de 2 metros quadrados.
#DESAFIO 17

l = float(input("Digite a largura da parede: "))
a = float(input("Digite a alutra da parede: "))
area = l * a
tinta = area / 2
print("Temos uma área de {}m²\n para pintar essa parede voce vai precisar"
      "de {}de tinta".format(area,tinta))


