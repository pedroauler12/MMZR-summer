import pandas as pd

df_raw_simp = pd.read_excel(
    'Estudo_de_Garantias_v3.xlsx',
    sheet_name='Simplificado',
    header=None
)

header_row = 3
ticker_row = 2
data_start = header_row + 1

header_positions = [
    col for col, val in enumerate(df_raw_simp.iloc[header_row]) 
    if val == '%PL'
]

blocks = []
for start in header_positions:
    ticker = df_raw_simp.iat[ticker_row, start + 3]
    if pd.isna(ticker):
        continue

    block = df_raw_simp.iloc[data_start:, start:start + 5].copy()
    cols = df_raw_simp.iloc[header_row, start:start + 5].tolist()
    cols = ['Nota' if c == 'Notas' else c for c in cols]
    block.columns = cols
    block['Fundo'] = ticker
    blocks.append(block)

df_tidy_simp = pd.concat(blocks, ignore_index=True)
df_tidy_simp = df_tidy_simp.dropna(subset=['%PL']).reset_index(drop=True)

df_tidy_simp['%PL']   = df_tidy_simp['%PL'].astype(float)
df_tidy_simp['Norm.'] = df_tidy_simp['Norm.'].astype(float)
df_tidy_simp['Nota']  = df_tidy_simp['Nota'].astype(float)

df_tidy_simp = df_tidy_simp[['Fundo', '%PL', 'Norm.', 'Ativo', 'Garantia', 'Nota']].head(100)

print(df_tidy_simp.head(100))
