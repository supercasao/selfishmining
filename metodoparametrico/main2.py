import pandas as pd
import numpy as np

def calcular_SMT(df, periodo_T, n_permutacoes):
    # Convertendo 'time' para período de tempo
    df['time'] = pd.to_datetime(df['time'])
    df['period'] = df['time'].dt.to_period(periodo_T)

    mineradores = df['guessed_miner'].unique()
    SMT_values = {}

    for minerador in mineradores:
        SMT_values[minerador] = []

        for periodo in df['period'].unique():
            df_periodo = df[df['period'] == periodo]
            blocos_descobertos = df_periodo[df_periodo['guessed_miner'] == minerador].shape[0]

            # Simulações de permutação
            permutacoes = []
            for _ in range(n_permutacoes):
                df_permutado = df_periodo.copy()
                df_permutado['guessed_miner'] = np.random.permutation(df_permutado['guessed_miner'].values)
                blocos_permutados = df_permutado[df_permutado['guessed_miner'] == minerador].shape[0]
                permutacoes.append(blocos_permutados)

            S_T_i = np.mean(permutacoes)
            sigma_S_T_i = np.std(permutacoes)

            if sigma_S_T_i != 0:
                SMT_i = (blocos_descobertos - S_T_i) / sigma_S_T_i
            else:
                SMT_i = 0  # Para evitar divisão por zero

            SMT_values[minerador].append(SMT_i)

    return SMT_values

def identificar_selfish_miners(SMT_values, criterio=2):
    selfish_miners = []

    for minerador, SMT_list in SMT_values.items():
        for SMT_i in SMT_list:
            if SMT_i > criterio:
                selfish_miners.append(minerador)
                break  # Se identificou uma vez, não precisa checar os outros períodos

    return selfish_miners

def main():
    # Exemplo de dados (substitua com seus próprios dados)
    data = {
        'guessed_miner': np.random.choice(['Miner1', 'Miner2', 'Miner3'], 100),
        'time': pd.date_range('2023-01-01', periods=100, freq='D'),
    }
    df = pd.DataFrame(data)

    periodo_T = 'M'  # período mensal
    n_permutacoes = 1000

    SMT_values = calcular_SMT(df, periodo_T, n_permutacoes)
    selfish_miners = identificar_selfish_miners(SMT_values)

    print("Selfish Miners identificados:")
    print(selfish_miners)

if __name__ == "__main__":
    main()
