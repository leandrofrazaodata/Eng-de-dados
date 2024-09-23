import datetime
import requests
import os
import json
from pathlib import Path


class Collector:
    def __init__(self, url: str, subject: str):
        """
        Inicializa a classe com a URL da API e o assunto para o caminho de armazenamento.
        
        :param url: URL da API a ser consumida
        :param subject: Assunto usado para nomear diretórios
        """
        self.url = url
        self.subject = subject

    def get_request(self) -> dict:
        """
        Faz uma requisição GET à API e retorna os dados em formato JSON.

        :return: Dados retornados pela API em formato JSON
        :raises: Uma exceção em caso de falha na requisição
        """
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()  
            print("Sucesso na requisição da API")
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição da API: {e}")
            raise

    def ensure_directory_exists(self, path: str):
        """
        Verifica se o diretório existe, caso não exista, será criado.

        :param path: Caminho do diretório
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        print(f"Diretório {path} verificado/criado.")

    def save_raw(self, data: dict) -> bool:
        """
        Salva os dados recebidos da API em um arquivo JSON com timestamp.

        :param data: Dados a serem salvos
        :return: True se o arquivo for salvo com sucesso
        """

        file_name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")

        path = f'/home/leandro/Eng-de-dados/data/{self.subject}/raw/'

        self.ensure_directory_exists(path)


        file_path = os.path.join(path, f'{file_name}.json')
        with open(file_path, 'w') as file:
            json.dump(data, file)
        print(f"Dados salvos em: {file_path}")

        return True

    def get_save(self) -> dict:
        """
        Faz a requisição à API, salva os dados brutos na camada raw e os retorna,
        caso seja necessário para alguma outra operação.

        :return: Dados retornados pela API
        """
        data = self.get_request()
        self.save_raw(data)
        return data



