#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mapear_codigo_cli.py
--------------------
Traduz tokens das colunas G1..Gn que correspondam a "Tipos de Garantia"
para o respectivo CÓDIGO oficial (AF, CF, FR, etc.) com base na planilha
de Classificação.

Uso (MASTER):
    python mapear_codigo_cli.py \
        --input data/garantias_limpas_MASTER.csv \
        --saida data/garantias_cod_MASTER.csv \
        --classif data/Estudo_de_Garantias_v3.xlsx

Uso legado (sem args):
    python mapear_codigo_cli.py
"""

import pandas as pd
import argparse
from pathlib import Path
from unidecode import unidecode
import re

# ------------------------------------------------------------------
# normalizar (reimplementado para independência do script de limpeza)
# ------------------------------------------------------------------
def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
ap = argparse.ArgumentParser(description="Mapeia tokens G* para códigos oficiais.")
ap.add_argument("--input", default="data/garantias_limpas.csv",
                help="CSV de tokens limpos.")
ap.add_argument("--saida", default="data/garantias_cod.csv",
                help="CSV de saída com códigos mapeados.")
ap.add_argument("--classif", default="data/Estudo_de_Garantias_v3.xlsx",
                help="Planilha de Classificação oficial.")
args = ap.parse_args()

ARQ_CLASS = Path(args.classif)
ARQ_IN    = Path(args.input)
ARQ_OUT   = Path(args.saida)

print(f"→ Gerando dicionários a partir de {ARQ_CLASS}")
df_class = pd.read_excel(ARQ_CLASS, sheet_name='Classificação', header=1, dtype=str)

# Conjuntos oficiais
CODIGOS_OFICIAIS     = set(df_class['Código'].dropna().str.upper())
SUBCLASSES_OFICIAIS  = {normalizar(s) for s in df_class['Subclasse'].dropna()}

# Map Tipo → Código
ALIAS2CODE = {}
for _, row in df_class[['Tipos de Garantia','Código']].dropna().iterrows():
    alias = normalizar(row['Tipos de Garantia'])
    code  = str(row['Código']).upper().strip()
    ALIAS2CODE.setdefault(alias, code)  # mantém o primeiro

# Ajustes comuns (siglas)
ADICIONAIS = {
    'fr': 'FR',
    'cs': 'CS',
    'r' : 'R',
    'p' : 'P',   # alguns penhores abreviados
    'h' : 'H',
    'gl': 'GL',
    'a' : 'A',
    'f' : 'F',
}
ALIAS2CODE.update(ADICIONAIS)

print(f"Encontrados {len(ALIAS2CODE)} aliases → código e {len(SUBCLASSES_OFICIAIS)} subclasses oficiais.")

# ------------------------------------------------------------------
# Ler tokens limpos
# ------------------------------------------------------------------
print(f"→ Lendo {ARQ_IN}")
df = pd.read_csv(ARQ_IN, dtype=str)

token_cols = [c for c in df.columns if c.startswith('G')]

def traduz_token(tok):
    """Se token for tipo de garantia (alias), retorna código; senão mantém original."""
    if pd.isna(tok) or not isinstance(tok, str) or tok == "":
        return tok
    key = normalizar(tok)
    return ALIAS2CODE.get(key, tok)

for col in token_cols:
    df[col] = df[col].apply(traduz_token)

# ------------------------------------------------------------------
# Salvar
# ------------------------------------------------------------------
df.to_csv(ARQ_OUT, index=False)
print(f" Tokens mapeados salvos em: {ARQ_OUT}")

print("\nAmostra das primeiras 25 linhas depois do mapeamento:")
print(df.head(25))
