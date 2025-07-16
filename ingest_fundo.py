#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# --------------------------------------------------------------
# Parse CLI
# --------------------------------------------------------------
ap = argparse.ArgumentParser(description="Ingestão de um único fundo e geração de CSV staging p/ MASTER.")
ap.add_argument("arquivo",      help="Arquivo Excel de entrada (caminho).")
ap.add_argument("nome_fundo",   help="Ticker do fundo (ex.: KNIP11).")
ap.add_argument("sheet",        help="Nome exato da aba no Excel.")
ap.add_argument("header", type=int,
                help="Número da linha de cabeçalho (0-index) para o pd.read_excel.")
ap.add_argument("--outdir", default=".", help="Diretório de saída para raw/staging.")
args = ap.parse_args()

ARQ_XLS   = Path(args.arquivo)
FUNDO     = args.nome_fundo
SHEET     = args.sheet
HDR       = args.header
OUTDIR    = Path(args.outdir)
OUTDIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------
# Ler Excel
# --------------------------------------------------------------
df = pd.read_excel(ARQ_XLS, sheet_name=SHEET, header=HDR)
df = df.dropna(axis=1, how='all')  # remove colunas totalmente vazias

# --------------------------------------------------------------
# Detectar coluna de percentual
# --------------------------------------------------------------
possiveis_colunas = ['% DA CARTEIRA', '% DO PL', '%DO PL', '%PL', '% DO PATRIMÔNIO', '% DO PATRIMONIO']
col_perc = [c for c in df.columns if c.strip().upper() in {p.upper() for p in possiveis_colunas}]
if not col_perc:
    raise ValueError("Nenhuma coluna de percentual encontrada no arquivo!")
perc_col = col_perc[0]
df = df.rename(columns={perc_col: '% DA CARTEIRA'})

# --------------------------------------------------------------
# Fatiar colunas principais (tolerante a variações)
# --------------------------------------------------------------
# Tabelinha de sinônimos simples (sem transformar tudo — vamos devagar)
colmap = {}
for c in df.columns:
    cu = c.strip().upper()
    if cu in ('ATIVO', 'TIPO ATIVO', 'TIPO'):
        colmap[c] = 'ATIVO'
    elif cu in ('CÓDIGO DO ATIVO', 'CODIGO DO ATIVO', 'CÓDIGO', 'CODIGO', 'CÓDIGO CRI', 'CÓDIGO CRI/CRA', 'CÓDIGO DO CRI'):
        colmap[c] = 'CÓDIGO DO ATIVO'
    elif cu in ('GARANTIAS', 'GARANTIA', 'DESCRIÇÃO GARANTIA', 'DESCRICAO GARANTIA'):
        colmap[c] = 'GARANTIAS'
    elif cu == '% DA CARTEIRA':
        colmap[c] = '% DA CARTEIRA'

df = df.rename(columns=colmap)

needed = ['ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'GARANTIAS']
missing = [c for c in needed if c not in df.columns]
if missing:
    raise ValueError(f"Colunas obrigatórias ausentes após renomear: {missing}")

df = df[needed]

# --------------------------------------------------------------
# Filtrar para CRI (tolerante: contém 'CRI' no texto)
# --------------------------------------------------------------
df = df[df['ATIVO'].astype(str).str.upper().str.contains('CRI', na=False)].copy()

# --------------------------------------------------------------
# Converter % DA CARTEIRA para float
# --------------------------------------------------------------
def _to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    s = s.replace('%','')
    s = s.replace('.','').replace(',','.')
    try:
        v = float(s)
    except ValueError:
        return np.nan
    # se parece porcentagem > 1 e <= 100, converte p/ fração
    if v > 1.0 and v <= 100.0:
        v = v / 100.0
    return v

df['% DA CARTEIRA'] = df['% DA CARTEIRA'].apply(_to_float)

# --------------------------------------------------------------
# Norm.
# --------------------------------------------------------------
total = df['% DA CARTEIRA'].sum(skipna=True)
df['Norm.'] = df['% DA CARTEIRA'] / total if total and not np.isnan(total) else np.nan

# --------------------------------------------------------------
# Nome do fundo (repete)
# --------------------------------------------------------------
df['NOME DO FUNDO'] = FUNDO

# Reordena colunas no layout pedido
df = df[['NOME DO FUNDO', 'ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'Norm.', 'GARANTIAS']]

# --------------------------------------------------------------
# Salvar
# --------------------------------------------------------------
arq_raw  = OUTDIR / f"{FUNDO}_ingest_raw.xlsx"
arq_csv  = OUTDIR / f"{FUNDO}_staging.csv"

# salva raw excel
try:
    import xlsxwriter  # noqa
    engine_name = "xlsxwriter"
except ImportError:
    engine_name = "openpyxl"

with pd.ExcelWriter(arq_raw, engine=engine_name) as xlw:
    df.to_excel(xlw, sheet_name="raw", index=False)

df.to_csv(arq_csv, index=False)

# --------------------------------------------------------------
# Relatório curto
# --------------------------------------------------------------
print("\n=== INGESTÃO CONCLUÍDA (v2) ===")
print(f"Arquivo origem: {ARQ_XLS}")
print(f"Fundo:          {FUNDO}")
print(f"Aba:            {SHEET}")
print(f"Linhas válidas: {len(df)}")
print(f"Soma % DA CARTEIRA: {df['% DA CARTEIRA'].sum():.6f}")
print(f"Soma Norm.:         {df['Norm.'].sum():.6f}")
print(f"Saída raw:          {arq_raw}")
print(f"Saída staging:      {arq_csv}\n")

print("Prévia (raw ordenado):")
print(df.head(10).to_string(index=False))
