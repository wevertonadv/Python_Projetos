import re

from spellchecker import SpellChecker
from nltk.corpus import wordnet
import nltk

nltk.download('wordnet')


class DinamicSearchJSON:

    @staticmethod
    def set_keywords_synsets_dict(keywords_list: list):
        """
        Cria um dicionário com as palavras-chave de uma lista e seus synsets correspondentes.

        Args:
            keywords_list (list): Uma lista de palavras-chave.

        Returns:
            Um dicionário com as palavras-chave da lista e seus synsets correspondentes.
        """

        # Cria um dicionário vazio para armazenar as palavras-chave e seus synsets
        keywords_synsets_dict = {}

        # Cria uma instância do corretor ortográfico
        spell = SpellChecker()

        # Itera sobre cada palavra-chave na lista
        for keyword in keywords_list:
            # Converte a palavra-chave para minúsculas se ela estiver em maiúsculas
            if keyword.isupper():
                keyword = keyword.lower()

            # Converte a palavra-chave para snake_case
            key = re.sub('([A-Z])', r'_\1', keyword).lower()

            # Separa as palavras da palavra-chave por underscore
            list_key = key.split("_")

            # Cria um dicionário vazio para armazenar as palavras da palavra-chave e seus synsets correspondentes
            separated_words_dict = {}

            # Itera sobre cada palavra da palavra-chave separada
            for item in list_key:

                try:
                    # Obtém o synset correspondente da palavra usando o wordnet
                    word_corrected = spell.correction(item)
                    if word_corrected is not None:
                        separated_words_dict[item] = wordnet.synsets(spell.correction(item))[0]
                    else:
                        separated_words_dict[item] = wordnet.synsets(item)[0]
                except:
                    # Se não for possível obter o synset, adiciona None ao dicionário
                    separated_words_dict[item] = None

            # Adiciona a palavra-chave e seu dicionário correspondente ao dicionário principal
            keywords_synsets_dict[keyword] = separated_words_dict

        return keywords_synsets_dict

    @staticmethod
    def get_json_keys(json_obj, key_set):
        """
        Captura as chaves de um objeto JSON recursivamente e adiciona-as a um conjunto.

        Args:
            json_obj (dict/list): O objeto JSON a ser analisado.
            key_set (set): O conjunto que armazenará as chaves encontradas.

        Returns:
            None
        """

        # Se o objeto for um dicionário, iteramos sobre as chaves e valores
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                # Adicionamos a chave ao conjunto
                key_set.add(key)
                # Chamamos recursivamente a função para o valor correspondente
                DinamicSearchJSON.get_json_keys(value, key_set)

        # Se o objeto for uma lista, iteramos sobre os itens
        elif isinstance(json_obj, list):
            for item in json_obj:
                # Chamamos recursivamente a função para cada item
                DinamicSearchJSON.get_json_keys(item, key_set)

    @staticmethod
    def combine_synsets_and_map_similarity(dict1, dict2):
        """
        Combina todos os synsets de dois dicionários e calcula a similaridade entre cada par.

        Args:
            dict1 (dict): O primeiro dicionário contendo synsets.
            dict2 (dict): O segundo dicionário contendo synsets.

        Returns:
            Um dicionário contendo as chaves combinadas e suas similaridades correspondentes.
        """
        # Cria um dicionário vazio para armazenar os synsets combinados e suas similaridades
        combined_synsets = {}

        def wup_similarity(synset1, synset2):
            """
            Calcula a similaridade semântica entre dois synsets usando a medida WUP.

            Args:
                synset1 (nltk.corpus.reader.wordnet.Synset): O primeiro synset a ser comparado.
                synset2 (nltk.corpus.reader.wordnet.Synset): O segundo synset a ser comparado.

            Returns:
                A medida de similaridade WUP entre os dois synsets.
            """
            # Calcula e retorna a similaridade WUP entre os dois synsets
            return synset1.wup_similarity(synset2)

        # Itera sobre cada par de chaves e valores nos dois dicionários
        for key1, value1 in dict1.items():
            for key2, value2 in dict2.items():
                # Itera sobre cada synset nos valores dos dicionários
                for synset1 in value1.values():
                    for synset2 in value2.values():
                        # Se ambos os synsets não forem None, calcula a similaridade entre eles
                        if synset1 is not None and synset2 is not None:
                            sim = wup_similarity(synset1, synset2)
                            # Se a similaridade for maior que 0.9, adiciona a chave combinada e a similaridade ao dicionário
                            if sim > 0.9:
                                combined_key = key1 + '!' + key2
                                combined_synsets[combined_key] = sim

        return combined_synsets

    @staticmethod
    def get_keyword_and_keys_semantic_similarity(keywords_list: list, json_object):

        """
        Calcula a similaridade semântica entre as palavras-chave de uma lista e as chaves de um objeto JSON.

        Args:
            keywords_list (list): Uma lista de palavras-chave.
            json_object (dict/list): O objeto JSON a ser comparado.

        Returns:
            Um dicionário com as chaves combinadas e suas similaridades correspondentes.
        """

        # Cria uma lista com todas as chaves do objeto JSON
        key_set = set()
        DinamicSearchJSON.get_json_keys(json_object, key_set)
        list_of_keys = list(key_set)

        # Cria um dicionário com os synsets correspondentes a cada chave do objeto JSON
        keys_synsets_dict = DinamicSearchJSON.set_keywords_synsets_dict(list_of_keys)

        # Cria um dicionário com os synsets correspondentes a cada palavra-chave da lista
        keywords_synsets_dict = DinamicSearchJSON.set_keywords_synsets_dict(keywords_list)
        # Combina os synsets dos dois dicionários e calcula a similaridade entre eles
        return DinamicSearchJSON.combine_synsets_and_map_similarity(keywords_synsets_dict, keys_synsets_dict)

    @staticmethod
    def extrair_info_api_por_chaves_similares(
            prefixo_rotulo: str,
            json_api: dict,
            dict_palavras_chave: list,
            store_id: str,
            data_extracao,
    ):
        """
        Extrai informações de um dicionário (JSON) de uma API recursivamente, comparando as chaves do dicionário com uma lista de palavras-chave e mapeando as informações relevantes.

        Args:
            prefixo_rotulo (str): O prefixo a ser adicionado aos rótulos de cada item extraído.
            json_api (dict): O dicionário (JSON) extraído do request da API a ser analisado.
            dict_palavras_chave (list): A lista contendo as palavras-chave a serem comparadas com as chaves do dicionário (JSON) da API.
            store_id (str): O ID da loja, se houver.
            data_extracao (datetime): A data de extração das informações.

        Returns:
            Um dicionário contendo as informações extraídas (dict_exportar) e uma lista de dicionários contendo as informações extraídas que estão aninhadas (lista_exportar).
        """

        # Inicializa os dicionários e listas necessárias
        dict_exportar = {}
        lista_itens_info = []
        lista_exportar = []

        # Itera sobre as chaves e valores do dicionário (JSON) fornecido
        for chave, valor in json_api.items():

            for chave_combinada in dict_palavras_chave.keys():
                lista_palavras_combinadas = chave_combinada.split("!")

                # Verifica se a chave está presente na lista_palavras_combinadas
                if chave in lista_palavras_combinadas:

                    # Trata os tipos de valor: int, str, float e bool
                    if isinstance(valor, (int, str, float, bool)):
                        etiqueta = prefixo_rotulo + chave
                        dict_exportar[etiqueta] = valor
                        etiqueta += "_"

                        # Trata os dicionários aninhados, chamando a função recursivamente
                    elif isinstance(valor, dict):
                        etiqueta = prefixo_rotulo + chave
                        tupla_itens_dict = DinamicSearchJSON.extrair_info_api_por_chaves_similares(
                            etiqueta + "_",
                            valor,
                            dict_palavras_chave,
                            store_id,
                            data_extracao
                        )
                        lista_itens_info += tupla_itens_dict[1]
                        dict_exportar.update(tupla_itens_dict[0])

                        # Adiciona informações de store_id e data_extracao, se disponíveis
                        if store_id is not None:
                            dict_exportar.update({'store_id': store_id, 'utc_extraction_date': data_extracao})
                        else:
                            dict_exportar.update({'utc_extraction_date': data_extracao})

                    # Trata listas de valores
                    elif isinstance(valor, list):
                        contador = 0
                        for item in valor:
                            contador += 1
                            # Trata dicionários dentro da lista, chamando a função recursivamente
                            if isinstance(item, dict):
                                etiqueta = prefixo_rotulo + chave + "_"
                                tupla_info_itens = DinamicSearchJSON.extrair_info_api_por_chaves_similares(
                                    etiqueta,
                                    item,
                                    dict_palavras_chave,
                                    store_id,
                                    data_extracao
                                )
                                dict_itens = tupla_info_itens[0]
                                lista_itens = tupla_info_itens[1]
                                dict_itens.update(dict_exportar)

                                # Adiciona informações de store_id e data_extracao, se disponíveis
                                if store_id is not None:
                                    dict_exportar.update({'store_id': store_id, 'utc_extraction_date': data_extracao})
                                else:
                                    dict_exportar.update({'utc_extraction_date': data_extracao})

                                lista_itens_info.append(dict_itens)
                                lista_itens_info += lista_itens

                            # Trata os tipos de valor: int, str, float e bool dentro da lista
                            elif isinstance(item, (int, str, float, bool)):
                                etiqueta = prefixo_rotulo + chave + "item_" + str(contador)
                                dict_exportar[etiqueta] = item

        # Atualiza e adiciona informações à lista_exportar, se houver itens na lista_itens_info
        if len(lista_itens_info) != 0:
            for item_info in lista_itens_info:
                item_info.update(dict_exportar)
                lista_exportar.append(item_info)

        # Retorna os dicionários e listas de informações extraídas
        return dict_exportar, lista_exportar