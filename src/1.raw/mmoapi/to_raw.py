from utils.ingestion_raw import Collector



def main():
    url = 'https://www.mmobomb.com/api1/games'
    subject = url.rstrip('/').split('/')[-1]
    data_collector = Collector(url=url, subject=subject)
    try:
        data_collector.get_save()
        print("Processo concluído com sucesso!")
    except Exception as e:
        print(f"Erro ao executar o processo: {e}")
if __name__ == "__main__":
    main()


