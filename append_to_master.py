#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_to_master.py
-------------------
Adiciona (ou substitui) um fundo novo no MASTER financeiro.

Uso:
    python append_to_master.py \
      --new-csv input_dados/KNIP11_staging.csv \
      --master data/df_tidy_simp_MASTER.csv \
      --saida data/df_tidy_simp_MASTER.csv \
      --replace-existing

Se omitido --replace-existing, o script ERRA caso o Fundo já exista no MASTER
(pra evitar duplicar).
"""

import argparse
import pandas as pd
from pathlib import Path

ap = argparse.ArgumentParser(description="Append/replace de um fundo no MASTER financeiro.")
ap.add_argument("--new-csv", required=True, help="CSV staging do novo fundo.")
ap.add_argument("--master",  required=True, help="MASTER financeiro atual.")
ap.add_argument("--saida",   required=True, help="Caminho de saída para novo MASTER.")
ap.add_argument("--replace-existing", action="store_true",
                help="Se fornecido, remove linhas existentes do fundo antes de anexar.")
args = ap.parse_args()

NEW   = Path(args.new_csv)
MASTER= Path(args.master)
SAIDA = Path(args.saida)

df_new = pd.read_csv(NEW)
# padronizar nomes de colunas → converter para esquema MASTER
colmap = {
    'NOME DO FUNDO': 'Fundo',
    'CÓDIGO DO ATIVO': 'Ativo',
    '% DA CARTEIRA': '%PL',
    'GARANTIAS': 'Garantia',
}
df_new = df_new.rename(columns=colmap)
# garante colunas mínimas
need = ['Fundo','Ativo','%PL','Norm.','Garantia']
missing = [c for c in need if c not in df_new.columns]
if missing:
    raise ValueError(f"Novo CSV sem colunas: {missing}")

if MASTER.exists():
    df_master = pd.read_csv(MASTER)
else:
    df_master = pd.DataFrame(columns=need)

# substituição?
fundo = df_new['Fundo'].iloc[0]
if not args.replace_existing and (df_master['Fundo'] == fundo).any():
    raise RuntimeError(f"Fundo {fundo} já existe no MASTER; use --replace-existing.")
if args.replace_existing:
    df_master = df_master[df_master['Fundo'] != fundo]

df_out = pd.concat([df_master, df_new[need + [c for c in df_new.columns if c not in need]]],
                   ignore_index=True)

df_out.to_csv(SAIDA, index=False)
print(f"MASTER atualizado salvou {len(df_out)} linhas em: {SAIDA}")
