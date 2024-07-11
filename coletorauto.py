import os
import requests
import gzip
from urllib.parse import urljoin
from pymongo import MongoClient
from calendar import monthrange


def baixar_e_extrair_mensalmente(ano_inicio, ano_fim, mes_inicio, mes_fim, diretorio_download, diretorio_extracao):
    # Cria os diretórios se não existirem
    os.makedirs(diretorio_download, exist_ok=True)
    os.makedirs(diretorio_extracao, exist_ok=True)

    base_url = 'https://gz.blockchair.com/bitcoin/blocks/'

    # Lista para armazenar os nomes dos arquivos a serem baixados
    arquivos_para_baixar = []

    # Loop pelos anos e meses especificados
    for ano in range(ano_inicio, ano_fim + 1):
        for mes in range(mes_inicio, mes_fim + 1):
            # Descobrir o último dia do mês atual
            ultimo_dia_do_mes = monthrange(ano, mes)[1]

            # Loop pelos dias do mês
            for dia in range(1, ultimo_dia_do_mes + 1):
                nome_arquivo = f"blockchair_bitcoin_blocks_{ano}{mes:02d}{dia:02d}.tsv.gz"
                url = urljoin(base_url, nome_arquivo)

                # Verifica se o arquivo existe
                response = requests.head(url)
                if response.status_code == 200:
                    arquivos_para_baixar.append(nome_arquivo)
                else:
                    break  # Interrompe o loop dos dias se o arquivo não existir

    # Baixa e extrai os arquivos encontrados
    arquivos_extraidos = []
    for nome_arquivo in arquivos_para_baixar:
        url = urljoin(base_url, nome_arquivo)
        caminho_arquivo_tsv_gz = os.path.join(diretorio_download, nome_arquivo)

        # Baixa o arquivo .tsv.gz
        response = requests.get(url)
        with open(caminho_arquivo_tsv_gz, 'wb') as f:
            f.write(response.content)

        # Verifica se o arquivo .tsv.gz foi baixado corretamente
        if os.path.exists(caminho_arquivo_tsv_gz):
            # Extrai o conteúdo do arquivo .tsv.gz
            arquivo_extraido = extrair_tsv_de_gzip(caminho_arquivo_tsv_gz, diretorio_extracao)
            if arquivo_extraido:
                arquivos_extraidos.append(arquivo_extraido)
                # Insere os dados no MongoDB
                inserir_no_mongodb(arquivo_extraido)
            else:
                print(f"Erro ao extrair ou processar {caminho_arquivo_tsv_gz}")

            # Remove o arquivo .tsv.gz após extração
            os.remove(caminho_arquivo_tsv_gz)

    return arquivos_extraidos


# Função para extrair o conteúdo de um arquivo .tsv.gz
def extrair_tsv_de_gzip(caminho_arquivo_tsv_gz, diretorio_extracao):
    try:
        with gzip.open(caminho_arquivo_tsv_gz, 'rb') as f_in:
            nome_arquivo_tsv = os.path.splitext(os.path.basename(caminho_arquivo_tsv_gz))[0]
            caminho_arquivo_extraido = os.path.join(diretorio_extracao, nome_arquivo_tsv)

            with open(caminho_arquivo_extraido, 'wb') as f_out:
                f_out.write(f_in.read())

            return caminho_arquivo_extraido
    except Exception as e:
        print(f"Erro ao extrair {caminho_arquivo_tsv_gz}: {e}")
        return None


# Função para inserir os dados no MongoDB
def inserir_no_mongodb(arquivo):
    try:
        # Conexão ao MongoDB (ajuste conforme seu ambiente)
        client = MongoClient('localhost', 27017)
        db = client['nome_do_banco']  # Nome do seu banco de dados
        collection = db['nome_da_colecao']  # Nome da sua coleção

        # Leitura dos dados do arquivo e inserção no MongoDB
        with open(arquivo, 'r') as f:
            next(f)  # Pular cabeçalho se necessário
            for line in f:
                # Processamento dos dados conforme necessário
                # Por exemplo, dividir a linha em campos
                fields = line.strip().split('\t')
                # Montar o documento a ser inserido no MongoDB
                document = {
                    'field1': fields[0],
                    'field2': fields[1],
                    # Adicione mais campos conforme necessário
                }
                # Inserir documento na coleção
                collection.insert_one(document)

        print(f"Dados inseridos no MongoDB: {arquivo}")

        # Fechar conexão
        client.close()
    except Exception as e:
        print(f"Erro ao inserir no MongoDB: {e}")


# Função principal
if __name__ == "__main__":
    ano_inicio = int(input("Digite o ano inicial: "))
    ano_fim = int(input("Digite o ano final: "))
    mes_inicio = int(input("Digite o mês inicial: "))
    mes_fim = int(input("Digite o mês final: "))

    diretorio_download = 'C://downloads'
    diretorio_extracao = 'C://ciencia_da_computacao_ufu//pythonProject//selfishmining'

    arquivos_txt = baixar_e_extrair_mensalmente(ano_inicio, ano_fim, mes_inicio, mes_fim, diretorio_download,
                                                diretorio_extracao)

    if arquivos_txt:
        print(f"Arquivos .txt encontrados:")
        for arquivo_txt in arquivos_txt:
            print(arquivo_txt)
    else:
        print("Nenhum arquivo .txt encontrado ou erro ao processar os arquivos.")
