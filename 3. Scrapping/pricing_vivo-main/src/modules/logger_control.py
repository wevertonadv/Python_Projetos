import os
import datetime
import subprocess
import glob
import __main__
class Logger:
    def __init__(self, nome_arquivo="log"):
        self.nome_arquivo = nome_arquivo
        self.caminho = os.path.join(os.getcwd(), "log")
        self.criar_pasta_log()
        self.arquivo = self.criar_arquivo_log()

    def criar_pasta_log(self):
        if not os.path.exists(self.caminho):
            os.mkdir(self.caminho)

    def criar_arquivo_log(self):
        #nome_arquivo = f"{self.nome_arquivo}_{datetime.datetime.now().strftime('%d%m%Y_%H%M%S')}.log"
        nome_arquivo = f"{self.nome_arquivo}.log" #arquivo unico 
        caminho_arquivo = os.path.join(self.caminho, nome_arquivo)
        return open(caminho_arquivo, "a", encoding="utf-8")

    def escrever_log(self, mensagem):
        data_hora = datetime.datetime.now().strftime("[%d/%m/%Y %H:%M:%S]")
        self.arquivo.write(f"{data_hora} {mensagem}\n")
        self.arquivo.flush()
        print(mensagem)
        return mensagem

    def info(self, mensagem):
        return self.escrever_log(f"[INFO] {mensagem}\n")
    def warning(self, mensagem):
        return self.escrever_log(f"[WARNING] {mensagem}\n")
    def error(self, mensagem):
        return self.escrever_log(f"[ERROR] {mensagem}\n")