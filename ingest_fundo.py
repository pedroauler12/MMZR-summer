#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from rapidfuzz import process, fuzz

# --------------------------------------------------------------
# Parse CLI
# --------------------------------------------------------------
ap = argparse.ArgumentParser(description="Ingestão de um único fundo e geração de CSV staging p/ MASTER.")
ap.add_argument("arquivo", help="Arquivo Excel de entrada (caminho).")
ap.add_argument("nome_fundo", help="Ticker do fundo (ex.: KNIP11).")
ap.add_argument("sheet", help="Nome exato da aba no Excel.")
ap.add_argument("header", type=int, help="Número da linha de cabeçalho (0-index).")
ap.add_argument("--outdir", default=".", help="Diretório de saída para raw/staging.")
args = ap.parse_args()

ARQ_XLS = Path(args.arquivo)
FUNDO = args.nome_fundo
SHEET = args.sheet
HDR = args.header
OUTDIR = Path(args.outdir)
OUTDIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------
# Ler Excel
# --------------------------------------------------------------
df = pd.read_excel(ARQ_XLS, sheet_name=SHEET, header=HDR)
df = df.dropna(axis=1, how='all')

# --------------------------------------------------------------
# Função de fuzzy‑match de colunas
# --------------------------------------------------------------
def fuzzy_match_column(df, candidatos, threshold=80):
    for candidato in candidatos:
        match, score, _ = process.extractOne(
            candidato, df.columns, scorer=fuzz.token_sort_ratio
        )
        if score >= threshold:
            return match
    return None

# --------------------------------------------------------------
# Detectar coluna de percentual
# --------------------------------------------------------------
possiveis_colunas_perc = [
    '% DA CARTEIRA', '% DO PL', '%DO PL', '%PL',
    '% DO PATRIMÔNIO', '% DO PATRIMONIO',
    '%/PL', 'PCT PL', 'PCT/PL'
]
col_perc = fuzzy_match_column(df, possiveis_colunas_perc)
if col_perc is None:
    # fallback simples por substring
    cands = [c for c in df.columns if '%' in c and 'PL' in c.upper()]
    if cands:
        col_perc = cands[0]
if not col_perc:
    print("Colunas encontradas:", list(df.columns))
    raise ValueError("Nenhuma coluna de percentual encontrada no arquivo!")
df = df.rename(columns={col_perc: '% DA CARTEIRA'})

# --------------------------------------------------------------
# Detectar colunas principais: ATIVO, CÓDIGO DO ATIVO, GARANTIAS
# --------------------------------------------------------------
possiveis_ativo = ['ATIVO', 'TIPO ATIVO', 'TIPO', 'TIPO LASTRO', 'CLASSE', 'ESPECIE']
possiveis_cod = [
    'CÓDIGO DO ATIVO', 'CODIGO DO ATIVO', 'CÓDIGO', 'CODIGO',
    'CÓDIGO CRI', 'CÓDIGO CRI/CRA', 'CÓDIGO DO CRI'
]
possiveis_garantia = ['GARANTIAS', 'GARANTIA', 'DESCRIÇÃO GARANTIA', 'DESCRICAO GARANTIA']

col_ativo = fuzzy_match_column(df, possiveis_ativo)
if col_ativo is None:
    # se não achou, assume tudo CRI
    df['ATIVO'] = 'CRI'
    col_ativo = 'ATIVO'

col_cod = fuzzy_match_column(df, possiveis_cod)
if col_cod is None:
    cands = [c for c in df.columns if 'CÓDIGO' in c.upper()]
    if cands:
        col_cod = cands[0]
if not col_cod:
    print("Colunas encontradas:", list(df.columns))
    raise ValueError("Nenhuma coluna de código do ativo encontrada!")

col_garantia = fuzzy_match_column(df, possiveis_garantia)
if col_garantia is None:
    cands = [c for c in df.columns if 'GARANTIA' in c.upper()]
    if cands:
        col_garantia = cands[0]
if not col_garantia:
    print("Colunas encontradas:", list(df.columns))
    raise ValueError("Nenhuma coluna de garantia encontrada!")

df = df.rename(columns={
    col_ativo: 'ATIVO',
    col_cod: 'CÓDIGO DO ATIVO',
    col_garantia: 'GARANTIAS'
})

needed = ['ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'GARANTIAS']
df = df[needed]

# --------------------------------------------------------------
# Remover rodapé: descarta tudo a partir da 1ª linha sem código
# --------------------------------------------------------------
mask_sem_codigo = df['CÓDIGO DO ATIVO'].isna()
if mask_sem_codigo.any():
    cutoff = mask_sem_codigo.idxmax()
    df = df.loc[:cutoff-1].copy()

# --------------------------------------------------------------
# Filtrar apenas CRIs
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
    s = str(x).strip().replace('%','')
    s = s.replace('.','').replace(',','.')
    try:
        v = float(s)
    except ValueError:
        return np.nan
    return v/100.0 if 1.0 < v <= 100.0 else v

df['% DA CARTEIRA'] = df['% DA CARTEIRA'].apply(_to_float)

# --------------------------------------------------------------
# Normalizar pesos, adicionar nome do fundo e reordenar
# --------------------------------------------------------------
total = df['% DA CARTEIRA'].sum(skipna=True)
df['Norm.'] = df['% DA CARTEIRA'] / total if total and not np.isnan(total) else np.nan
df['NOME DO FUNDO'] = FUNDO
df = df[['NOME DO FUNDO', 'ATIVO', 'CÓDIGO DO ATIVO', '% DA CARTEIRA', 'Norm.', 'GARANTIAS']]

# --------------------------------------------------------------
# Salvar
# --------------------------------------------------------------
arq_raw = OUTDIR / f"{FUNDO}_ingest_raw.xlsx"
arq_csv = OUTDIR / f"{FUNDO}_staging.csv"
try:
    import xlsxwriter  # noqa
    eng = "xlsxwriter"
except ImportError:
    eng = "openpyxl"

with pd.ExcelWriter(arq_raw, engine=eng) as xlw:
    df.to_excel(xlw, sheet_name="raw", index=False)
df.to_csv(arq_csv, index=False)

# --------------------------------------------------------------
# Relatório curto
# --------------------------------------------------------------
print("\n=== INGESTÃO CONCLUÍDA ===")
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
