import sys
from json_to_parquet_converter import JsonToParquetConverter

def main(input_dir: str, output_dir: str):
    """
    Função principal que instancia o conversor e executa a conversão.

    Args:
        input_dir (str): Caminho do diretório de entrada.
        output_dir (str): Caminho do diretório de saída.
    """
    try:
        converter = JsonToParquetConverter(input_dir, output_dir)
        converter.convert()
        print("Conversão concluída com sucesso!")
    except Exception as e:
        print(f"Erro ao executar a conversão: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python main.py <diretorio_de_entrada> <diretorio_de_saida>")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    main(input_directory, output_directory)