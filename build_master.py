#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Constrói dataset MASTER (base + novos fundos).")
parser.add_argument("--base", required=True, help="CSV base atual (df_tidy_simp.csv).")
parser.add_argument("--staging", nargs="+", required=True, help="Um ou mais CSVs de staging.")
parser.add_argument("--out", default="data/df_tidy_simp_MASTER.csv", help="Caminho de saída do MASTER.")
parser.add_argument("--replace-existing", action="store_true",
                    help="Se passar, remove linhas do mesmo Fundo já existentes no base.")
args = parser.parse_args()

PATH_BASE = Path(args.base)
PATH_OUT  = Path(args.out)

df_base = pd.read_csv(PATH_BASE)

dfs_new = []
for p in args.staging:
    p = Path(p)
    if not p.exists():
        print(f" staging não encontrado: {p}")
        continue
    df = pd.read_csv(p)
    dfs_new.append(df)

if not dfs_new:
    raise SystemExit("Nenhum staging válido lido; abortando.")

df_new_all = pd.concat(dfs_new, ignore_index=True)

EXPECTED = ["Fundo","Ativo","%PL","Norm.","Garantia","Nota"]

# Normalizar colunas base
for c in EXPECTED:
    if c not in df_base.columns:
        df_base[c] = np.nan
df_base = df_base[EXPECTED + [c for c in df_base.columns if c not in EXPECTED]]

# Normalizar colunas novos
for c in EXPECTED:
    if c not in df_new_all.columns:
        df_new_all[c] = np.nan
df_new_all = df_new_all[EXPECTED + [c for c in df_new_all.columns if c not in EXPECTED]]

if args.replace_existing:
    fundos_new = set(df_new_all["Fundo"].dropna().unique())
    mask_keep  = ~df_base["Fundo"].isin(fundos_new)
    dropped    = (~mask_keep).sum()
    if dropped:
        print(f"→ Removendo {dropped} linhas do base (fundos já existentes: {fundos_new})")
    df_base = df_base[mask_keep]

df_master = pd.concat([df_base, df_new_all], ignore_index=True)

# segurança: coagir numéricos
df_master["%PL"]   = pd.to_numeric(df_master["%PL"], errors="coerce")
df_master["Norm."] = pd.to_numeric(df_master["Norm."], errors="coerce")
df_master["Nota"]  = pd.to_numeric(df_master["Nota"], errors="coerce")

df_master.to_csv(PATH_OUT, index=False)

print("\n=== MASTER CONSTRUÍDO ===")
print(f"Base original: {df_base.shape}")
print(f"Novos fundos : {df_new_all.shape}")
print(f"MASTER final : {df_master.shape}")
print(f"Salvo em: {PATH_OUT}")
print("\nFundos no MASTER:", df_master['Fundo'].unique())
