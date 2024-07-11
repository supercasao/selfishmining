import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
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


def calcular_estatisticas(df):
    """Calcula estatísticas usando as diferenças absolutas entre os tempos de mineração dos blocos."""
    delta_times = df['delta_time'].to_numpy()

    if len(delta_times) > 1:
        tempo_medio = np.mean(delta_times)
        mediana_tempo = np.median(delta_times)
        desvio_padrao_tempo = np.std(delta_times)
    else:
        tempo_medio, mediana_tempo, desvio_padrao_tempo = 0.0, 0.0, 0.0

    return tempo_medio, mediana_tempo, desvio_padrao_tempo


def calcular_estatisticas_mensais(df):
    """Calcula estatísticas mensais para todo o dataset."""
    df['month'] = df['time'].dt.to_period('M')
    estatisticas_mensais = df.groupby('month')['delta_time'].agg(['mean', 'median', 'std'])
    estatisticas_mensais.columns = ['Média Mensal', 'Mediana Mensal', 'Desvio Padrão Mensal']
    return estatisticas_mensais


def identificar_pools_selfish_mining(df, limiar_selfish=0.1):
    """Identifica pools suspeitas de mineração egoísta."""
    estatisticas_pool = df['guessed_miner'].value_counts(normalize=True)
    pools_selfish = estatisticas_pool[estatisticas_pool > limiar_selfish]
    return pools_selfish


def identificar_pools_selfish_mining_mensal(df, limiar_selfish=0.1):
    """Identifica pools suspeitas de mineração egoísta mensalmente."""
    df['month'] = df['time'].dt.to_period('M')
    pools_suspeitas_mensal = {}

    for month, group in df.groupby('month'):
        estatisticas_pool = group['guessed_miner'].value_counts(normalize=True)
        pools_selfish = estatisticas_pool[estatisticas_pool > limiar_selfish]
        pools_suspeitas_mensal[month] = pools_selfish

    return pools_suspeitas_mensal


def calcular_poder_computacional_geral(df, pools_suspeitas):
    """Calcula o poder computacional das pools suspeitas para todo o período."""
    total_blocos = df.shape[0]
    poder_geral = df['guessed_miner'].value_counts().loc[pools_suspeitas.index]
    poder_geral = poder_geral / total_blocos
    return poder_geral


def calcular_poder_computacional_mes(df, pools_suspeitas):
    """Calcula o poder computacional das pools suspeitas por mês."""
    df['month'] = df['time'].dt.to_period('M')
    poder_mensal = df.groupby(['month', 'guessed_miner']).size().unstack(fill_value=0).astype(float)

    for month in poder_mensal.index:
        total_blocos_mes = poder_mensal.loc[month].sum()
        poder_mensal.loc[month] = poder_mensal.loc[month] / total_blocos_mes

    poder_mensal_suspeitas = poder_mensal[pools_suspeitas.index]

    return poder_mensal_suspeitas


def contar_blocos_consecutivos(df, pool):
    """Conta o número de blocos consecutivos minerados por uma pool específica e o total de blocos minerados."""
    df_pool = df[df['guessed_miner'] == pool].reset_index(drop=True)
    contagem_consecutivos = 0

    if len(df_pool) < 2:
        return contagem_consecutivos, len(df_pool)

    for i in range(1, len(df_pool)):
        if (df_pool.loc[i, 'time'] - df_pool.loc[i - 1, 'time']).seconds <= 600:
            contagem_consecutivos += 1

    total_blocos_minerados = df_pool.shape[0]
    return contagem_consecutivos, total_blocos_minerados


def plotar_poder_computacional(poder_mensal_suspeitas):
    """Plota a variação do poder computacional das pools suspeitas ao longo dos meses."""
    plt.figure(figsize=(14, 7))
    for pool in poder_mensal_suspeitas.columns:
        plt.plot(poder_mensal_suspeitas.index.astype(str), poder_mensal_suspeitas[pool], marker='o', label=pool)

    plt.xlabel('Mês')
    plt.ylabel('Poder Computacional (%)')
    plt.title('Variação do Poder Computacional das Pools Suspeitas de Selfish Mining')
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plotar_estatisticas_e_gaussiana(df, pools_suspeitas, estatisticas_mensais):
    """Plota a média, mediana e curva gaussiana geral e para cada pool suspeita."""
    delta_times = df['delta_time'].to_numpy()

    # Calcular média e desvio padrão geral
    media_geral = np.mean(delta_times)
    desvio_padrao_geral = np.std(delta_times, ddof=1)

    for pool in pools_suspeitas.index:
        delta_times_pool = df[df['guessed_miner'] == pool]['delta_time'].to_numpy()
        media_pool = np.mean(delta_times_pool)
        desvio_padrao_pool = np.std(delta_times_pool, ddof=1)

        plt.figure(figsize=(14, 7))

        # Plotar distribuição geral
        xmin, xmax = np.min(delta_times), np.max(delta_times)
        x = np.linspace(xmin, xmax, 100)
        p = norm.pdf(x, media_geral, desvio_padrao_geral)
        plt.plot(x, p, linewidth=2, color='blue', label='Curva Gaussiana Geral')

        # Plotar distribuição da pool
        p_pool = norm.pdf(x, media_pool, desvio_padrao_pool)
        plt.plot(x, p_pool, linewidth=2, color='red', label=f'Curva Gaussiana {pool}')

        plt.title(f'Comparação de Distribuições Gaussianas: Geral vs {pool}')
        plt.xlabel('Delta Time (s)')
        plt.ylabel('Densidade de Probabilidade')
        plt.legend()
        plt.grid(True)
        plt.show()


def plotar_poder_computacional_diario(df, pools_suspeitas_mensal):
    """Plota a variação diária do poder computacional das pools suspeitas mais significativas em cada mês."""
    df['date'] = df['time'].dt.to_period('D')

    for mes, pools in pools_suspeitas_mensal.items():
        if not pools.empty:
            pool_mais_significativa = pools.idxmax()
            df_mes = df[df['time'].dt.to_period('M') == mes]
            poder_diario = df_mes.groupby(['date', 'guessed_miner']).size().unstack(fill_value=0)
            poder_diario = poder_diario / poder_diario.sum(axis=1).values[:, None]

            plt.figure(figsize=(14, 7))
            plt.plot(poder_diario.index.astype(str), poder_diario[pool_mais_significativa], marker='o', label=pool_mais_significativa)
            plt.xlabel('Dia')
            plt.ylabel('Poder Computacional (%)')
            plt.title(f'Variação Diária do Poder Computacional: {pool_mais_significativa} em {mes}')
            plt.legend()
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.tight_layout()
            plt.show()


def permutar_dados(df, num_permutacoes=1000):
    """Permuta os dados `num_permutacoes` vezes e retorna uma lista de DataFrames permutados."""
    dfs_permutados = []
    for _ in range(num_permutacoes):
        df_permutado = df.copy()
        df_permutado['guessed_miner'] = df['guessed_miner'].sample(frac=1).values
        dfs_permutados.append(df_permutado)

    return dfs_permutados

def calcular_e_plotar_analises(df):
    # Calcular estatísticas gerais
    tempo_medio, mediana_tempo, desvio_padrao_tempo = calcular_estatisticas(df)
    print(f'Estatísticas Gerais:')
    print(f'  Média do tempo de mineração: {tempo_medio:.2f} segundos')
    print(f'  Mediana do tempo de mineração: {mediana_tempo:.2f} segundos')
    print(f'  Desvio padrão do tempo de mineração: {desvio_padrao_tempo:.2f} segundos\n\n')

    # Calcular estatísticas mensais
    estatisticas_mensais = calcular_estatisticas_mensais(df)
    print('Estatísticas Mensais:')
    print(estatisticas_mensais)
    print('\n\n')

    # Identificar pools suspeitas
    pools_suspeitas = identificar_pools_selfish_mining(df)
    print('Pools Suspeitas (Geral):')
    print(pools_suspeitas)
    print('\n\n')

    # Calcular poder computacional das pools suspeitas para todo o período
    poder_geral_suspeitas = calcular_poder_computacional_geral(df, pools_suspeitas)
    print('Poder Computacional Geral das Pools Suspeitas:')
    print(poder_geral_suspeitas)
    print('\n\n')

    # Calcular poder computacional das pools suspeitas por mês
    poder_mensal_suspeitas = calcular_poder_computacional_mes(df, pools_suspeitas)
    print('Poder Computacional Mensal das Pools Suspeitas:')
    print(poder_mensal_suspeitas)
    print('\n\n')

    # Contar blocos consecutivos minerados pelas pools suspeitas e total de blocos minerados
    print('Contagem de Blocos Consecutivos e Totais Minerados (Geral):')
    for pool in pools_suspeitas.index:
        contagem_consecutivos, total_blocos_minerados = contar_blocos_consecutivos(df, pool)
        total_blocos_disponiveis = df.shape[0]
        print(f'  {pool}:')
        print(f'    Blocos consecutivos: {contagem_consecutivos}')
        print(f'    Total de blocos minerados: {total_blocos_minerados}')
        print(f'    Total de blocos disponíveis: {total_blocos_disponiveis}')
        print('\n')
    print('\n\n')

    # Plotar variação do poder computacional das pools suspeitas ao longo dos meses
    plotar_poder_computacional(poder_mensal_suspeitas)

    # Plotar curvas gaussianas comparando as pools suspeitas com os dados gerais
    plotar_estatisticas_e_gaussiana(df, pools_suspeitas, estatisticas_mensais)

    # Identificar pools suspeitas mensalmente
    pools_suspeitas_mensal = identificar_pools_selfish_mining_mensal(df)
    print('Pools Suspeitas (Mensal):')
    for mes, pools in pools_suspeitas_mensal.items():
        print(f'  {mes}: {pools.index.tolist()}')
    print('\n\n')

    # Plotar variação diária do poder computacional das pools suspeitas mais significativas em cada mês
    plotar_poder_computacional_diario(df, pools_suspeitas_mensal)


def analise_mineracao_consecutiva_original(df):
    """Analisa mineração consecutiva na amostra original."""
    mineradores = df['guessed_miner'].unique()
    qtd_mineradores = len(mineradores)
    df_sorted = df.sort_values('time')
    lmc = {minerador: 0 for minerador in mineradores}

    for i in range(1, len(df_sorted)):
        if df_sorted.iloc[i]['guessed_miner'] == df_sorted.iloc[i - 1]['guessed_miner']:
            lmc[df_sorted.iloc[i]['guessed_miner']] += 1

    return lmc


def analise_mineracao_consecutiva_permutada(df, num_permutacoes=1000):
    """Analisa mineração consecutiva na amostra permutada."""
    mineradores = df['guessed_miner'].unique()
    qtd_mineradores = len(mineradores)
    lmc_permutado = []

    for _ in range(num_permutacoes):
        df_permutado = df.copy()
        df_permutado['guessed_miner'] = np.random.permutation(df_permutado['guessed_miner'].values)
        df_sorted = df_permutado.sort_values('time')
        lmc = {minerador: 0 for minerador in mineradores}

        for i in range(1, len(df_sorted)):
            if df_sorted.iloc[i]['guessed_miner'] == df_sorted.iloc[i - 1]['guessed_miner']:
                lmc[df_sorted.iloc[i]['guessed_miner']] += 1

        lmc_permutado.append(lmc)

    return lmc_permutado


def comparar_analises(originais, permutados):
    """Compara a análise original com as análises permutadas."""
    mineradores = originais.keys()
    num_permutacoes = len(permutados)
    resultados = {minerador: 0 for minerador in mineradores}

    for minerador in mineradores:
        original_count = originais[minerador]
        permutado_counts = [permutado[minerador] for permutado in permutados]
        resultados[minerador] = sum(1 for count in permutado_counts if original_count > count)

    return resultados


def main():
    # Definir nome do banco de dados e coleção no MongoDB
    nome_banco = 'nome_do_banco_de_dados'
    nome_colecao = 'nome_da_colecao'

    # Ler dados do MongoDB
    df = ler_dados_do_mongodb(nome_banco, nome_colecao)

    if df is None or df.empty:
        print("Não foi possível ler dados válidos do MongoDB.")
        return

    # Limpar e processar dados originais
    df_original = limpar_e_processar_dados(df)

    # Calcular e plotar análises para os dados originais
    calcular_e_plotar_analises(df_original)

    print("#######################################################################################")

    # Algoritmo 1: Análise na amostra original
    lmc_original = analise_mineracao_consecutiva_original(df_original)
    print(f'Contagem de minerações consecutivas na amostra original: {lmc_original}')

    # Algoritmo 2: Análise nas amostras permutadas
    num_permutacoes = 1000
    lmc_permutado = analise_mineracao_consecutiva_permutada(df_original, num_permutacoes=num_permutacoes)
    print(
        f'Contagem de minerações consecutivas nas amostras permutadas (Exemplo da primeira permutação): {lmc_permutado[0]}')

    # Algoritmo 3: Comparação das análises original e permutadas
    resultados_comparacao = comparar_analises(lmc_original, lmc_permutado)
    print(f'Resultados da comparação não paramétrica: {resultados_comparacao}')


if __name__ == "__main__":
    main()