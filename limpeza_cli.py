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

def load_classificacao(path_class):
    """
    Lê a tabela de classificação a partir de um Excel que pode ter:
      - Nome de aba variando (Classificação / Classificacao / Sheet1 / etc.)
      - Linha de cabeçalho deslocada (formatação, título, etc.)

    Estratégia:
      1. Abrimos o arquivo com pd.ExcelFile.
      2. Para cada aba, carregamos sem cabeçalho (header=None).
      3. Normalizamos strings e procuramos uma linha que contenha >=2
         nomes esperados: {tipos de garantia, código, subclasse, nota}.
      4. Quando acharmos, relermos a aba com header=linha_detectada.
      5. Retornamos dfClass e os sets normalizados necessários para a limpeza.

    Se nada encontrado, levantamos erro amigável.
    """

    import pandas as pd
    import re
    from unidecode import unidecode

    def _norm(s):
        return re.sub(r"\s+", " ", unidecode(str(s)).strip().lower())

    EXPECTED = {
        "tipos de garantia": "Tipos de Garantia",
        "codigo": "Código",
        "código": "Código",
        "subclasse": "Subclasse",
        "nota": "Nota",
    }

    xl = pd.ExcelFile(path_class)
    chosen_sheet = None
    chosen_header = None

    for sh in xl.sheet_names:
        tmp = xl.parse(sh, header=None, dtype=str)
        # varre linhas procurando cabeçalho
        for i, row in tmp.iterrows():
            # normaliza todas as células da linha
            normed = [_norm(v) for v in row.tolist()]
            # quantas colunas "esperadas" aparecem nessa linha?
            hits = sum(1 for v in normed if v in EXPECTED)
            if hits >= 2:  # achamos linha candidata
                chosen_sheet = sh
                chosen_header = i
                break
        if chosen_sheet is not None:
            break

    if chosen_sheet is None:
        raise ValueError(
            f"Não encontrei nenhuma aba em '{path_class}' com cabeçalho compatível "
            "(esperado algo contendo 'Tipos de Garantia', 'Código', 'Subclasse', 'Nota')."
        )

    if chosen_sheet not in ("Classificação", "Classificacao"):
        print(f"[WARN] Tabela Classificação detectada na aba '{chosen_sheet}' (header linha {chosen_header}).")

    # Lê novamente a aba correta com o header detectado
    dfClass = xl.parse(chosen_sheet, header=chosen_header)

    # --- Derivados para o resto da limpeza ---
    tipos_garantia = set(dfClass['Tipos de Garantia'].dropna().astype(str).str.strip().unique()) \
        if 'Tipos de Garantia' in dfClass.columns else set()

    codigo = set(dfClass['Código'].dropna().astype(str).str.strip().unique()) \
        if 'Código' in dfClass.columns else set()

    subclasses = set(dfClass['Subclasse'].dropna().astype(str).str.strip().unique()) \
        if 'Subclasse' in dfClass.columns else set()

    # normalizador local (ou importe o global, se preferir)
    def normalizar(s: str) -> str:
        if not isinstance(s, str):
            return s
        s = unidecode(s).lower()
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    tipos_norm = {normalizar(t) for t in tipos_garantia}
    codigos    = {c.upper() for c in codigo}
    subs_norm  = {normalizar(s) for s in subclasses}
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
                path_class: Path = Path('data/Estudo_de_Garantias_v3.xlsx')):
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
    ap.add_argument("--classif", default="data/Estudo_de_Garantias_v3.xlsx",
                    help="Planilha de Classificação.")
    args = ap.parse_args()

    run_limpeza(Path(args.input), Path(args.out_csv), Path(args.out_xlsx), Path(args.classif))

if __name__ == "__main__":
    main()


import pandas as pd
path = "data/Estudo_de_Garantias_v3.xlsx"
xls = pd.ExcelFile(path)
print("Arquivo lido:", path)
print("Abas encontradas:", xls.sheet_names)
df = xls.parse(sheet_name='Classificação', header=1)
