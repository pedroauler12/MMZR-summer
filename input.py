import pandas as pd
import argparse


parser = argparse.ArgumentParser(description="Processa dados de um fundo imobiliário e calcula pesos normalizados para CRIs.")
parser.add_argument('arquivo', help='Caminho do arquivo Excel de entrada')
parser.add_argument('nome_fundo', help='Nome do fundo para inserir na coluna final')
parser.add_argument('sheet', help='Nome da aba (sheet) no Excel')
parser.add_argument('header', type=int, help='Número da linha de cabeçalho (começando do zero)')
args = parser.parse_args()



df = pd.read_excel(args.arquivo, sheet_name=args.sheet, header=args.header)
df = df.dropna(axis=1, how='all')  # Remove columns with all NaN values

possiveis_colunas = ['% DA CARTEIRA', '% DO PL']
col_perc = [col for col in possiveis_colunas if col in df.columns]
if not col_perc:
    raise ValueError('Nenhuma coluna de percentual encontrada!')
df = df.rename(columns={col_perc[0]: '% DA CARTEIRA'})


# retirar colunas  que não sejam % DA CARTEIRA , GARANTIA  ATIVO e CODIGO do ATIVO
df = df[[ 'ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'GARANTIAS']]
# retirar as linhas que o ativo não é CRI
df = df[df['ATIVO'] == 'CRI']




def adiciona_peso (df):
    df = df.copy()
    total = df['% DA CARTEIRA'].sum()
    df['Norm.'] = df['% DA CARTEIRA'] / total
    return df


df = adiciona_peso(df)


#adicionar uma coluna com o nome do fundo , esta coluan deve ser repetir para todas as linhas , esse nome vai ser passado como variavel

df['NOME DO FUNDO'] = args.nome_fundo
df = df[['NOME DO FUNDO', 'ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'GARANTIAS', 'Norm.']]

print(df.head(20))
df.to_excel('PlanilhadeFundamentos_KIP.xlsx', index=False)
