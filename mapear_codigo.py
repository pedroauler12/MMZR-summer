#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
import pandas as pd
from pathlib import Path
from unidecode import unidecode
import re

# --------------------------------------------------------------
# CLI
# --------------------------------------------------------------
ap = argparse.ArgumentParser(description="Mapeia tokens limpos → códigos oficiais (MASTER).")
ap.add_argument("--limpas",  default="data/garantias_limpas_MASTER.csv",
                help="CSV limpo (fase 1).")
ap.add_argument("--classif", default="data/Estudo_de_Garantias_v3.xlsx",
                help="Planilha Classificação.")
ap.add_argument("--saida-csv", default="data/garantias_cod_MASTER.csv",
                help="CSV de saída.")
args = ap.parse_args()

ARQ_LIMPAS = Path(args.limpas)
ARQ_CLASS  = Path(args.classif)
ARQ_SAIDA  = Path(args.saida_csv)

# --------------------------------------------------------------
# Normalizar
# --------------------------------------------------------------
def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# --------------------------------------------------------------
# Ler Classificação → ALIAS2CODE
# --------------------------------------------------------------
print("→ Gerando dicionários a partir de", ARQ_CLASS)
df_class = pd.read_excel(ARQ_CLASS, sheet_name='Classificação', header=1, dtype=str)

CODIGOS_OFICIAIS     = set(df_class['Código'].dropna().str.upper())
SUBCLASSES_OFICIAIS  = {normalizar(s) for s in df_class['Subclasse'].dropna()}

ALIAS2CODE = {}
for _, row in df_class[['Tipos de Garantia', 'Código']].dropna().iterrows():
    alias = normalizar(row['Tipos de Garantia'])
    code  = str(row['Código']).upper().strip()
    ALIAS2CODE.setdefault(alias, code)

# Ajustes manuais úteis
ADICIONAIS = {
    'fr':  'FR',
    'cs':  'CS',
    'r':   'R',
    'spe': 'AF',  # cuidado! Se no limpas veio "spe" isolado como token de subclasse, NÃO traduzir a código.
                  # Por isso, NÃO adicionar 'spe' aqui a menos que saiba que é tipo → para já mapeado está falso.
}
# Observação: mantemos ADICIONAIS restrito. *Não* mapeamos SPE para AF.
ALIAS2CODE.update({k:v for k,v in ADICIONAIS.items() if k != 'spe'})

print(f"Encontrados {len(ALIAS2CODE)} aliases → código e {len(SUBCLASSES_OFICIAIS)} subclasses oficiais.")

# --------------------------------------------------------------
# Ler tokens limpos
# --------------------------------------------------------------
print("→ Lendo", ARQ_LIMPAS)
df = pd.read_csv(ARQ_LIMPAS, dtype=str)

token_cols = [c for c in df.columns if c.startswith('G')]

def traduz_token(tok):
    if pd.isna(tok) or not isinstance(tok, str):
        return tok
    key = normalizar(tok)
    return ALIAS2CODE.get(key, tok)

for col in token_cols:
    df[col] = df[col].apply(traduz_token)

# --------------------------------------------------------------
# Salvar
# --------------------------------------------------------------
print("→ Salvando resultado em", ARQ_SAIDA)
df.to_csv(ARQ_SAIDA, index=False)
print(df.head(25))
