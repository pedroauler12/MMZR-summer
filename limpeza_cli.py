#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path
from unidecode import unidecode

SPLIT_REGEX = r'\s*(?:\+|-|,|;|\bou\b|\be\b|\bem\b|\bde\b|\bda\b|\bdos\b|\bdo\b|\be/?ou\b|\(\w+\)|•)\s*'
TOKEN_SPLIT_RE = re.compile(r'^(?P<cod>[A-Za-z]{1,4})\s+(?P<rest>.+)$')

# ------------------------------------------------------------------
# Utilitários
# ------------------------------------------------------------------
def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def limpar_celula(x):
    if not isinstance(x, str):
        return x
    x = x.strip(" ;.•")
    x = re.sub(r'^\d+\s*', '', x)
    x = re.sub(r'\s*\d+$', '', x)
    return x.strip()

def load_classificacao(path_class: Path):
    dfClass = pd.read_excel(path_class, sheet_name='Classificação', header=1)
    tipos_garantia = set(dfClass['Tipos de Garantia'].dropna().str.strip().unique())
    codigo         = set(dfClass['Código'].dropna().str.strip().unique())
    subclasses     = set(dfClass['Subclasse'].dropna().str.strip().unique())

    tipos_norm  = {normalizar(t) for t in tipos_garantia}
    codigos     = {c.upper() for c in codigo}
    subs_norm   = {normalizar(s) for s in subclasses}
    prefix_tipo = {t.split(' ', 1)[0]: t for t in tipos_norm}
    return dfClass, tipos_norm, codigos, subs_norm, prefix_tipo

ALIAS = {
    'imoveis'        : 'imovel',
    'imovel'         : 'imovel',
    'alugueis'       : 'aluguéis',
    'aluguel'        : 'aluguéis',
    'graos'          : 'grãos/ producao',
    'grao'           : 'grãos/ producao',
    'terrenoa'       : 'terreno',
    'terrenos'       : 'terreno',
    'spes'           : 'spe',
    'spe'            : 'spe',
    'sobrecolateral' : 'sobrecolateral',
    'socios pessoa fisica': 'sócios',
    'socios pessoa juridica': 'sócios',
    'socios pf'      : 'sócios',
    'socios'         : 'sócios',
    'cotas'          : 'cotas de fundo (fip, fii etc.) e ações',
    'quotas'         : 'cotas de fundo (fip, fii etc.) e ações',
    'recebiveis'     : 'recebíveis/ crédito/ direito creditório',
    'cash'           : 'cash sweep',
    'fianca'         : 'fiança',
    'fianca locacao' : 'fiança',
    'fianca bancaria': 'fiança',
    'fianÇa'         : 'fiança',
}

def make_keep_token(tipos_norm, codigos, subs_norm, prefix_tipo, alias=ALIAS):
    def keep_token(token):
        if isinstance(token, list):
            out = []
            for item in token:
                out.extend(keep_token(item))
            return out
        if not isinstance(token, str) or token.strip() == "":
            return []
        t = normalizar(token)
        t = re.sub(r'[\d$\.]+', '', t)
        t = t.strip(',;() ')
        if t == '':
            return []
        if '%' in t:
            return []
        if t.startswith('reserva') and 'fundo' not in t:
            return []
        t = alias.get(t, t)
        t = normalizar(t)
        out = []
        m = TOKEN_SPLIT_RE.match(t)
        if m:
            cod = m.group('cod').upper()
            rest = normalizar(m.group('rest'))
            rest = alias.get(rest, rest)
            if rest in subs_norm or rest in tipos_norm:
                out.append(rest)
            else:
                for piece in rest.split():
                    piece = alias.get(piece, piece)
                    if piece in subs_norm or piece in tipos_norm:
                        out.append(piece)
            if cod in codigos:
                out.append(cod)
            return list(dict.fromkeys(out))
        first = t.split(' ', 1)[0]
        if first in prefix_tipo:
            return [prefix_tipo[first]]
        if t in tipos_norm or t in subs_norm:
            return [t]
        if token.upper() in codigos:
            return [token.upper()]
        return []
    return keep_token

# ------------------------------------------------------------------
# Função PRINCIPAL de limpeza (Fase 6)
# ------------------------------------------------------------------
def run_limpeza(path_in: Path,
                path_out_csv: Path,
                path_out_xlsx: Path,
                path_class: Path = Path('Estudo_de_Garantias_v3.xlsx')):
    """
    Limpa e tokeniza as garantias do arquivo de entrada.
    Salva em CSV e XLSX, retorna o DataFrame limpo.
    """
    df_original = pd.read_csv(path_in)
    df = df_original.copy()

    # mantemos só colunas mínimas
    if not {'Fundo','Ativo','Garantia'}.issubset(df.columns):
        raise ValueError(f"O arquivo {path_in} precisa conter colunas Fundo, Ativo, Garantia.")
    df = df[['Fundo','Ativo','Garantia']].copy()

    # limpa prefixos 'GARANTIAS -' etc
    df['Garantia'] = df['Garantia'].str.replace(r'^\s*(?:-+|•+|GARANTIAS)\s*', '', regex=True)

    # explode em cols Garantia_* usando regex de separação
    df_split = df['Garantia'].str.split(SPLIT_REGEX, expand=True)
    df_split = df_split.applymap(limpar_celula)
    df_split.columns = [f'Garantia_{i+1}' for i in range(df_split.shape[1])]
    df_split = pd.concat([df[['Fundo','Ativo']], df_split], axis=1)

    # dicionários oficiais
    dfClass, tipos_norm, codigos, subs_norm, prefix_tipo = load_classificacao(path_class)

    # bind keep_token
    keep_token = make_keep_token(tipos_norm, codigos, subs_norm, prefix_tipo, alias=ALIAS)

    # filtra tokens por linha
    gar_cols = df_split.filter(like='Garantia_')
    def filtra_linha(row):
        kept = []
        for cell in row.to_numpy():
            kept.extend(keep_token(cell))
        return pd.Series(kept)

    tmp = gar_cols.apply(filtra_linha, axis=1)
    tmp.columns = [f'G{i+1}' for i in range(tmp.shape[1])]
    df_clean = pd.concat([df_split[['Fundo','Ativo']], tmp], axis=1)

    # ruído para debug
    ruido = (
        gar_cols.stack().dropna().apply(lambda x: keep_token(x) == [])
    )
    print(f"Ruído remanescente: {ruido.mean():.2%}")

    # salva
    df_clean.to_csv(path_out_csv, index=False)
    df_clean.to_excel(path_out_xlsx, index=False)
    print(f"✅ Tokens limpos salvos em:\n  {path_out_csv}\n  {path_out_xlsx}")
    return df_clean

# ------------------------------------------------------------------
# CLI (main)
# ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Limpeza de garantias → tokens (G1..Gn).")
    ap.add_argument("--input",   default="data/df_tidy_simp.csv",
                    help="CSV de entrada (df_tidy_simp ou MASTER).")
    ap.add_argument("--out-csv", default="data/garantias_limpas.csv",
                    help="Saída CSV tokens.")
    ap.add_argument("--out-xlsx",default="data/garantias_limpas.xlsx",
                    help="Saída XLSX tokens.")
    ap.add_argument("--classif", default="Estudo_de_Garantias_v3.xlsx",
                    help="Planilha de Classificação.")
    args = ap.parse_args()

    run_limpeza(Path(args.input), Path(args.out_csv), Path(args.out_xlsx), Path(args.classif))

if __name__ == "__main__":
    main()
