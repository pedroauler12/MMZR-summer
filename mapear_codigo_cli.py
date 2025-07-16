#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mapear_codigo_cli.py
--------------------
Traduz tokens das colunas G1..Gn para códigos oficiais AF, CF, FR, etc.
Pode ser chamado como módulo ou script (CLI).
"""

import pandas as pd
from pathlib import Path
from unidecode import unidecode
import re
import argparse

def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def mapear_tokens(
    path_classif: Path = Path("Estudo_de_Garantias_v3.xlsx"),
    path_tokens: Path = Path("data/garantias_limpas.csv"),
    path_saida: Path = Path("data/garantias_cod.csv"),
    verbose: bool = True
) -> pd.DataFrame:
    """
    Mapeia tokens G* para códigos oficiais, salvando o resultado.
    Retorna o DataFrame convertido.
    """
    if verbose:
        print(f"→ Gerando dicionários a partir de {path_classif}")
    df_class = pd.read_excel(path_classif, sheet_name='Classificação', header=1, dtype=str)

    # Conjuntos oficiais
    CODIGOS_OFICIAIS     = set(df_class['Código'].dropna().str.upper())
    SUBCLASSES_OFICIAIS  = {normalizar(s) for s in df_class['Subclasse'].dropna()}

    # Map Tipo → Código
    ALIAS2CODE = {}
    for _, row in df_class[['Tipos de Garantia','Código']].dropna().iterrows():
        alias = normalizar(row['Tipos de Garantia'])
        code  = str(row['Código']).upper().strip()
        ALIAS2CODE.setdefault(alias, code)

    # Ajustes comuns (siglas)
    ADICIONAIS = {
        'fr': 'FR',
        'cs': 'CS',
        'r' : 'R',
        'p' : 'P',
        'h' : 'H',
        'gl': 'GL',
        'a' : 'A',
        'f' : 'F',
    }
    ALIAS2CODE.update(ADICIONAIS)

    if verbose:
        print(f"Encontrados {len(ALIAS2CODE)} aliases → código e {len(SUBCLASSES_OFICIAIS)} subclasses oficiais.")

    if verbose:
        print(f"→ Lendo {path_tokens}")
    df = pd.read_csv(path_tokens, dtype=str)

    token_cols = [c for c in df.columns if c.startswith('G')]

    def traduz_token(tok):
        """Se token for tipo de garantia (alias), retorna código; senão mantém original."""
        if pd.isna(tok) or not isinstance(tok, str) or tok == "":
            return tok
        key = normalizar(tok)
        return ALIAS2CODE.get(key, tok)

    for col in token_cols:
        df[col] = df[col].apply(traduz_token)

    # Salvar
    df.to_csv(path_saida, index=False)
    if verbose:
        print(f"Tokens mapeados salvos em: {path_saida}")
        print("\nAmostra das primeiras 25 linhas depois do mapeamento:")
        print(df.head(25))
    return df

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Mapeia tokens G* para códigos oficiais.")
    ap.add_argument("--input", default="data/garantias_limpas.csv",
                    help="CSV de tokens limpos.")
    ap.add_argument("--saida", default="data/garantias_cod.csv",
                    help="CSV de saída com códigos mapeados.")
    ap.add_argument("--classif", default="Estudo_de_Garantias_v3.xlsx",
                    help="Planilha de Classificação oficial.")
    args = ap.parse_args()

    mapear_tokens(
        path_classif=Path(args.classif),
        path_tokens=Path(args.input),
        path_saida=Path(args.saida),
        verbose=True
    )

if __name__ == "__main__":
    main()
