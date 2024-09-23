import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

class JsonToParquetConverter:
    """
    Classe para converter arquivos JSON em arquivos Parquet.

    Attributes:
        input_dir (str): Caminho do diretório de entrada contendo arquivos JSON.
        output_dir (str): Caminho do diretório de saída para os arquivos Parquet.
        spark (SparkSession): Instância do SparkSession.
    """

    def __init__(self, input_dir: str, output_dir: str):
        """
        Inicializa a classe JsonToParquetConverter.

        Args:
            input_dir (str): Diretório de entrada.
            output_dir (str): Diretório de saída.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.spark = SparkSession.builder \
            .appName("JsonToParquetConverter") \
            .getOrCreate()

    def convert(self):
        """
        Converte todos os arquivos JSON no diretório de entrada para o formato Parquet no diretório de saída.
        """
        os.makedirs(self.output_dir, exist_ok=True)

        # Itera sobre todos os arquivos no diretório de entrada
        for file_name in os.listdir(self.input_dir):
            if file_name.endswith('.json'):
                json_file_path = os.path.join(self.input_dir, file_name)
                parquet_temp_dir = os.path.join(self.output_dir, file_name.replace('.json', ''))

                try:
                    print(f"Lendo {json_file_path}")
                    # Lê o arquivo JSON
                    df = self.spark.read.json(json_file_path)
                    print(f"Salvando em {parquet_temp_dir}")
                    # Salva como Parquet em um diretório temporário
                    df.write.mode('overwrite').parquet(parquet_temp_dir)

                    # Move o arquivo gerado para o local correto
                    for part_file in os.listdir(parquet_temp_dir):
                        if part_file.startswith("part-") and part_file.endswith(".parquet"):
                            shutil.move(os.path.join(parquet_temp_dir, part_file), os.path.join(self.output_dir, file_name.replace('.json', '.parquet')))
                    # Remove o diretório temporário
                    shutil.rmtree(parquet_temp_dir)

                except AnalysisException as e:
                    print(f"Erro ao processar {json_file_path}: {e}")
                except Exception as e:
                    print(f"Erro inesperado ao processar {json_file_path}: {e}")

        self.spark.stop()