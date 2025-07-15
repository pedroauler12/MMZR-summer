# codigo_limpo.py
import pandas as pd

def _load_simplificado():
    df_raw = pd.read_excel(
        'Estudo_de_Garantias_v3.xlsx',
        sheet_name='Simplificado',
        header=None
    )
    header_row, ticker_row = 3, 2
    data_start = header_row + 1

    starts = [
        col for col, val in enumerate(df_raw.iloc[header_row])
        if val == '%PL'
    ]

    blocks = []
    for start in starts:
        ticker = df_raw.iat[ticker_row, start + 3]
        if pd.isna(ticker):
            continue
        block = df_raw.iloc[data_start:, start:start + 5].copy()
        cols = df_raw.iloc[header_row, start:start + 5].tolist()
        cols = ['Nota' if c == 'Notas' else c for c in cols]
        block.columns = cols
        block['Fundo'] = ticker
        blocks.append(block)

    df = pd.concat(blocks, ignore_index=True)
    df = df.dropna(subset=['%PL']).reset_index(drop=True)

    # garantir tipos corretos
    df['%PL']   = df['%PL'].astype(float)
    df['Norm.'] = df['Norm.'].astype(float)
    df['Nota']  = df['Nota'].astype(float)

    # definir ordem de colunas
    df = df[['Fundo','%PL','Norm.','Ativo','Garantia','Nota']]

    # exporta para CSV
    df.to_csv('df_tidy_simp.csv', index=False)
    df.to_excel('df_tidy_simp.xlsx')

    return df

# executa o carregamento e exportação assim que o módulo é importado
df_tidy_simp = _load_simplificado()
