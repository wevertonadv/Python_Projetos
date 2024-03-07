# Definir as taxas de crescimento das bactérias A e B em decimal
taxa_crescimento_a = 0.03
taxa_crescimento_b = 0.015

populacao_a = 4
populacao_b = 10

dias = 0

while populacao_a < populacao_b:
    populacao_a *= 1 + taxa_crescimento_a
    populacao_b *= 1 + taxa_crescimento_b
    dias += 1

print(f"Levará {dias} dias para a colônia de A ultrapassar ou igualar a colônia de B.")
