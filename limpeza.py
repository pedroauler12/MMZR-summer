#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
import pandas as pd
import numpy as np
import re
from unidecode import unidecode
from pathlib import Path

# --------------------------------------------------------------
# CLI
# --------------------------------------------------------------
ap = argparse.ArgumentParser(description="Limpa e tokeniza Garantias a partir de MASTER financeiro.")
ap.add_argument("--fin", default="data/df_tidy_simp_MASTER.csv",
                help="CSV financeiro MASTER.")
ap.add_argument("--classif", default="data/Estudo_de_Garantias_v3.xlsx",
                help="Planilha de Classificação (p/ referência de tipos/subclasses).")
ap.add_argument("--saida-csv", default="data/garantias_limpas_MASTER.csv",
                help="CSV de tokens limpos.")
ap.add_argument("--saida-xlsx", default="data/garantias_limpas_MASTER.xlsx",
                help="Excel opcional com tokens limpos.")
args = ap.parse_args()

ARQ_FIN    = Path(args.fin)
ARQ_CLASS  = Path(args.classif)
ARQ_SAIDA  = Path(args.saida_csv)
ARQ_SAIDAX = Path(args.saida_xlsx)


TOKEN_SPLIT_RE = re.compile(r'^(?P<cod>[A-Za-z]{1,4})\s+(?P<rest>.+)$')

# separadores de garantia
REGEX_SPLIT = r'\s*(?:\+|-|,|;|\bou\b|\be\b|\bem\b|\bde\b|\bda\b|\bdos\b|\bdo\b|\be/?ou\b|\(\w+\)|•)\s*'


df_class = pd.read_excel(ARQ_CLASS, sheet_name='Classificação', header=1)
df_original = pd.read_csv(ARQ_FIN)

# usamos só as colunas mínimas
df = df_original[['Fundo','Ativo','Garantia']].copy()

# remove prefixos tipo "- GARANTIAS ..." etc
df['Garantia'] = df['Garantia'].str.replace(r'^\s*(?:-+|•+|GARANTIAS)\s*', '', regex=True)

# --------------------------------------------------------------
# Limpeza célula a célula
# --------------------------------------------------------------
def limpar_celula(x):
    if not isinstance(x, str):
        return x
    x = x.strip(" ;.•")
    x = re.sub(r'^\d+\s*', '', x)
    x = re.sub(r'\s*\d+$', '', x)
    return x.strip()

df_split = df['Garantia'].str.split(REGEX_SPLIT, expand=True)
df_split = df_split.applymap(limpar_celula)
df_split.columns = [f'Garantia_{i+1}' for i in range(df_split.shape[1])]
df_split = pd.concat([df[['Fundo','Ativo']], df_split], axis=1)

# --------------------------------------------------------------
# Normalização básica & vocabulários oficiais
# --------------------------------------------------------------
def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

tipos_garantia = set(df_class['Tipos de Garantia'].dropna().str.strip().unique())
codigo = set(df_class['Código'].dropna().str.strip().unique())
subclasses = set(df_class['Subclasse'].dropna().str.strip().unique())

tipos_norm = {normalizar(t) for t in tipos_garantia}
codigos = {c.upper() for c in codigo}
subs_norm = {normalizar(s) for s in subclasses}
prefix_tipo = {t.split(' ', 1)[0]: t for t in tipos_norm}

# --------------------------------------------------------------
# Alias mínimos (plurais / acentos / abreviações)
# --------------------------------------------------------------
alias = {
    # Subclasses
    'imoveis':    'imovel',
    'imóvel':     'imovel',
    'imóveis':    'imovel',
    'alugueis':   'aluguéis',
    'aluguel':    'aluguéis',
    'graos':      'grãos',
    'grao':       'grãos',
    'terrenoa':   'terreno',
    'terrenos':   'terreno',
    'spes':       'spe',
    'sobrecolateral': 'sobrecolateral',

    # colapsar sócios variantes
    'sócios pessoa física': 'sócios',
    'sócios pessoa jurídica': 'sócios',
    'sócios pf': 'sócios',

    # Tipos (mnemônicos)
    'cash':   'cash sweep',
    'fianca': 'fiança',
    'fiança': 'fianca',  # bidirecional p/ pega variação
}

# --------------------------------------------------------------
# keep_token()
# --------------------------------------------------------------
def keep_token(token):
    """Converte token bruto em lista [tipo|subclasse|código] ou [] se descartar."""
    if isinstance(token, list):
        out = []
        for item in token:
            out.extend(keep_token(item))
        return out

    if not isinstance(token, str) or token.strip() == "":
        return []

    # limpeza básica
    t = normalizar(token)
    t = re.sub(r'[\d$\.]+', '', t)
    t = t.strip(',;() ')
    if t == '':
        return []

    # descarta percentuais ou 'reserva' (exceto fundo reserva, tratada adiante)
    if '%' in t:
        return []
    if t.startswith('reserva') and 'fundo' not in t:
        return []

    # alias global
    t = alias.get(t, t)
    t = normalizar(t)

    out = []

    # Caso "COD resto"
    m = TOKEN_SPLIT_RE.match(t)
    if m:
        cod = m.group('cod').upper()
        rest = normalizar(m.group('rest'))

        alias_rest = alias.get(rest, rest)
        if alias_rest in subs_norm:
            out.append(alias_rest)
        else:
            for piece in rest.split():
                piece_norm = alias.get(piece, piece)
                if piece_norm in subs_norm:
                    out.append(piece_norm)

        if cod in codigos:
            out.append(cod)

        return list(dict.fromkeys(out))

    # prefixo de tipo
    first = t.split(' ', 1)[0]
    if first in prefix_tipo:
        return [prefix_tipo[first]]

    # token = tipo ou subclasse direto
    if t in tipos_norm or t in subs_norm:
        return [t]

    # código puro
    if token.upper() in codigos:
        return [token.upper()]

    return []

# --------------------------------------------------------------
# Aplicar linha a linha
# --------------------------------------------------------------
gar_cols = df_split.filter(like='Garantia_')
tmp = gar_cols.apply(lambda r: pd.Series(sum((keep_token(x) for x in r.to_numpy()), [])), axis=1)
tmp.columns = [f'G{i+1}' for i in range(tmp.shape[1])]

df_clean = pd.concat([df_split[['Fundo','Ativo']], tmp], axis=1)

# --------------------------------------------------------------
# Métrica de ruído
# --------------------------------------------------------------
ruido = (gar_cols.stack()
                   .dropna()
                   .apply(lambda x: keep_token(x) == []))
print(f"Ruído remanescente: {ruido.mean():.2%}")

# --------------------------------------------------------------
# Salvar
# --------------------------------------------------------------
df_clean.to_csv(ARQ_SAIDA, index=False)
try:
    import xlsxwriter  # noqa
    engine_name = "xlsxwriter"
except ImportError:
    engine_name = "openpyxl"
with pd.ExcelWriter(ARQ_SAIDAX, engine=engine_name) as xlw:
    df_clean.to_excel(xlw, sheet_name="limpas", index=False)

print(f"Tokens limpos salvos em: {ARQ_SAIDA} (e {ARQ_SAIDAX})")
