class MathCalculators:
    @staticmethod
    def calcular_taxa_juros(pv_sem_juros, pv_com_juros, n_parcelas):
        """
        Calcula a taxa de juros mensal com base no valor total sem juros (pv_sem_juros),
        no valor total com juros (pv_com_juros) e no número total de parcelas (n_parcelas).

        :param pv_sem_juros: Valor presente sem juros.
        :param pv_com_juros: Valor presente com juros.
        :param n_parcelas: Número de parcelas.
        :return: Taxa de juros mensal em porcentagem.
        """
        # Calculando a taxa de juros mensal
        taxa_juros_mensal = (pv_com_juros / pv_sem_juros) ** (1 / n_parcelas) - 1

        # Converter a taxa de juros para uma porcentagem
        return taxa_juros_mensal * 100

if __name__ == "__main__":
    # Exemplo de uso da classe MathCalculators
    calculadora = MathCalculators()
    pv_sem_juros = 2699.00  # Valor total a ser pago sem juros
    pv_com_juros = 2734.09  # Valor total a ser pago com juros para 11 parcelas
    n_parcelas = 11  # Número de parcelas

    # Calcula a taxa de juros
    taxa_juros = calculadora.calcular_taxa_juros(pv_sem_juros, pv_com_juros, n_parcelas)
    print(f"A taxa de juros mensal é de aproximadamente {taxa_juros:.2f}%.")
