import pandas as pd
from pymongo import MongoClient

def ler_dados_do_mongodb(nome_banco, nome_colecao):
    """Lê os dados do MongoDB e retorna como DataFrame."""
    try:
        client = MongoClient('localhost', 27017)
        db = client[nome_banco]
        collection = db[nome_colecao]
        cursor = collection.find()

        df = pd.DataFrame(list(cursor))
        client.close()

        return df
    except Exception as e:
        print(f"Erro ao ler dados do MongoDB: {e}")
        return None


def limpar_e_processar_dados(df):
    """Limpa e processa os dados."""
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S')
    df = df.sort_values(by='time')

    # Calcula as diferenças de tempo totais entre blocos consecutivos em segundos
    df['delta_time'] = df['time'].diff().dt.total_seconds().abs()
    df = df.dropna(subset=['delta_time'])

    return df