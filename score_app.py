#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
import pandas as pd
import numpy as np
import re
from pathlib import Path
from unidecode import unidecode
__all__ = ["run_score"]


# ------------------------------------------------------------------
# Normalização de strings
# ------------------------------------------------------------------
def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ------------------------------------------------------------------
# Carrega Classificação → dicionários
# ------------------------------------------------------------------
def load_classificacao(path_xlsx: Path,
                       sheet_name: str = "Classificação"):
    """Retorna df_class + class_map + conjuntos auxiliares."""
    df_class = pd.read_excel(path_xlsx, sheet_name=sheet_name, header=1)

    # Limpeza básica
    df_class['Código']    = df_class['Código'].astype(str).str.strip().str.upper()
    df_class['Subclasse'] = df_class['Subclasse'].astype(str).str.strip()

    # Normalizado p/ chave
    df_class['Subclasse_norm'] = df_class['Subclasse'].apply(normalizar)
    df_class['Nota'] = pd.to_numeric(df_class['Nota'], errors='coerce')

    # Mapa (Código, Subclasse_norm) -> Nota
    class_map = (
        df_class
        .dropna(subset=['Código','Subclasse_norm'])
        .set_index(['Código','Subclasse_norm'])['Nota']
        .to_dict()
    )

    CODIGOS_OFICIAIS = set(df_class['Código'].dropna().unique())

    # normalizado -> forma canônica (como aparece na planilha)
    SUB_NORM2CANON = (
        df_class.dropna(subset=['Subclasse'])
                .set_index('Subclasse_norm')['Subclasse']
                .to_dict()
    )

    return df_class, class_map, CODIGOS_OFICIAIS, SUB_NORM2CANON


# ------------------------------------------------------------------
# Carrega financeiro MASTER
# ------------------------------------------------------------------
def load_fin(path_fin: Path) -> pd.DataFrame:
    df_fin = pd.read_csv(path_fin)

    # Checa colunas mínimas
    needed = {'Fundo','Ativo','%PL','Norm.','Garantia'}
    missing = needed - set(df_fin.columns)
    if missing:
        raise ValueError(f"Arquivo financeiro {path_fin} sem colunas: {missing}")

    # Converte %PL / Norm. p/ float
    for col in ['%PL','Norm.']:
        df_fin[col] = pd.to_numeric(df_fin[col], errors='coerce')

    # Qualquer coluna 'Nota' será ignorada
    if 'Nota' in df_fin.columns:
        pass  # não removo (pode ser útil p/ debug visual), mas não uso

    return df_fin


# ------------------------------------------------------------------
# Carrega tokens codificados
# ------------------------------------------------------------------
def load_tokens(path_tok: Path) -> pd.DataFrame:
    df_tok = pd.read_csv(path_tok, dtype=str)
    gcols = [c for c in df_tok.columns if c.startswith('G')]
    for c in gcols:
        df_tok[c] = df_tok[c].replace({'': np.nan})
    return df_tok


# ------------------------------------------------------------------
# Extrai códigos/subclasses de uma linha de tokens
# ------------------------------------------------------------------
def extract_codes_subs(row, gcols, CODIGOS_OFICIAIS, SUB_NORM2CANON):
    codes = []
    subs  = []
    for c in gcols:
        tok = row.get(c, np.nan)
        if not isinstance(tok, str) or tok.strip() == "":
            continue

        t_up   = tok.upper().strip()
        t_norm = normalizar(tok)

        # código direto?
        if t_up in CODIGOS_OFICIAIS:
            if t_up not in codes:
                codes.append(t_up)
            continue

        # subclasse?
        if t_norm in SUB_NORM2CANON:
            subcanon = SUB_NORM2CANON[t_norm]
            if subcanon not in subs:
                subs.append(subcanon)
            continue

        # ruído → ignora
        continue

    return codes, subs


# ------------------------------------------------------------------
# Seleciona nota para linha
# ------------------------------------------------------------------
def nota_para_linha(codes, subs, class_map):
    """
    Tenta todas as combinações (c,s) → Nota.
    Se nenhuma combinar, fallback: maior nota do código (ignore subclasse).
    Se nada ainda, retorna NaN.
    """
    notas = []
    for c in codes:
        for s in subs:
            n = class_map.get((c, normalizar(s)), np.nan)
            notas.append(n)
    notas_validas = [n for n in notas if not np.isnan(n)]

    if notas_validas:
        return float(np.nanmax(notas_validas))

    # fallback: melhor nota do código
    notas_code = []
    for (c0, s0), n0 in class_map.items():
        if c0 in codes and not np.isnan(n0):
            notas_code.append(n0)
    if notas_code:
        return float(np.nanmax(notas_code))

    return np.nan


# ------------------------------------------------------------------
# Calcula score por fundo
# ------------------------------------------------------------------
def calcular_scores(df_all: pd.DataFrame,
                    drop_na_score: bool = False,
                    drop_na_norm: bool = False):
    """
    Espera colunas: Fundo, Norm., Nota_calculada.
    """
    work = df_all.copy()

    if drop_na_norm:
        work = work.dropna(subset=['Norm.'])
    if drop_na_score:
        work = work.dropna(subset=['Nota_calculada'])

    work['Prod'] = work['Norm.'] * work['Nota_calculada']
    scores = (
        work.groupby('Fundo', sort=False)['Prod']
            .sum()
            .div(0.03)
            .rename('Score_Garantia')
    )
    return scores


# ------------------------------------------------------------------
# Monta DataFrame debug por linha
# ------------------------------------------------------------------
def build_debug_df(df_fin, df_tok, df_all, gcols):
    """
    Retorna DF com colunas úteis para inspecionar linhas:
    Fundo, Ativo, Norm., Garantia, Nota_calculada, codes, subs, G*...
    (Nota humana ignorada mesmo que exista no financeiro.)
    """
    dbg_cols = ['Fundo','Ativo','Norm.','Garantia']
    base = df_fin[dbg_cols].reset_index(drop=True)

    base['codes'] = df_all['codes']
    base['subs']  = df_all['subs']
    base['Nota_calculada'] = df_all['Nota_calculada']

    base = pd.concat([base, df_tok[gcols].reset_index(drop=True)], axis=1)
    return base


# ------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------
def run_score(path_fin: Path,
              path_tok: Path,
              path_classif: Path,
              saida_xlsx: Path,
              fundo_filter: str | None = None,
              drop_na_norm: bool = False,
              drop_na_score: bool = False):
    # --- carregar insumos
    df_fin = load_fin(path_fin)
    df_tok = load_tokens(path_tok)
    df_class, class_map, CODIGOS_OFICIAIS, SUB_NORM2CANON = load_classificacao(path_classif)

    # Filtrar fundos (debug)
    if fundo_filter is not None:
        df_fin = df_fin[df_fin['Fundo'] == fundo_filter].reset_index(drop=True)
        df_tok = df_tok[df_tok['Fundo'] == fundo_filter].reset_index(drop=True)

    # Check lengths
    if len(df_fin) != len(df_tok):
        raise ValueError(
            f"Número de linhas difere entre financeiro ({len(df_fin)}) e tokens ({len(df_tok)}). "
            "Reveja ingestão / limpeza."
        )

    # cria índice incremental dentro de cada fundo
    df_fin = df_fin.copy()
    df_tok = df_tok.copy()
    df_fin['_row'] = df_fin.groupby('Fundo').cumcount()
    df_tok['_row'] = df_tok.groupby('Fundo').cumcount()

    # merge 1:1 (pegando tokens G* da tabela de tokens)
    gcols = [c for c in df_tok.columns if c.startswith('G')]
    df_all = df_fin.merge(
        df_tok[['Fundo','_row'] + gcols],
        on=['Fundo','_row'],
        how='left',
        validate='1:1'
    ).drop(columns=['_row'])

    # extrair codes/subs (aplicando ao df_tok original — mesmo índice)
    codes_subs = df_tok.apply(
        lambda r: extract_codes_subs(r, gcols, CODIGOS_OFICIAIS, SUB_NORM2CANON),
        axis=1,
        result_type='expand'
    )
    codes_subs.columns = ['codes','subs']

    # alinhar
    df_all['codes'] = codes_subs['codes'].values
    df_all['subs']  = codes_subs['subs'].values

    # nota calculada
    df_all['Nota_calculada'] = df_all.apply(
        lambda r: nota_para_linha(r['codes'], r['subs'], class_map),
        axis=1
    )

    # scores
    scores = calcular_scores(df_all, drop_na_score=drop_na_score, drop_na_norm=drop_na_norm)
    df_scores = scores.reset_index()

    # debug linhas
    df_debug = build_debug_df(df_fin, df_tok, df_all, gcols)

    # stats
    stats_rows = []
    for f, g in df_all.groupby('Fundo', sort=False):
        n = len(g)
        n_no_codes = (g['codes'].apply(len) == 0).sum()
        n_no_subs  = (g['subs'].apply(len) == 0).sum()
        n_na_note  = g['Nota_calculada'].isna().sum()
        stats_rows.append({
            'Fundo': f,
            'Linhas': n,
            'Sem_codes': n_no_codes,
            'Sem_subs': n_no_subs,
            'Nota_calc_NaN': n_na_note,
            'Soma_Norm': g['Norm.'].sum(),
            'Score_calc': scores.loc[f] if f in scores.index else np.nan,
        })
    df_stats = pd.DataFrame(stats_rows)

    # salvar
    with pd.ExcelWriter(saida_xlsx, engine='xlsxwriter') as xlw:
        df_scores.to_excel(xlw, sheet_name='Scores', index=False)
        df_debug.to_excel(xlw, sheet_name='Debug_Linhas', index=False)
        df_stats.to_excel(xlw, sheet_name='Stats', index=False)

    # print resumo
    print(f"\n✅ Score calculado e salvo em: {saida_xlsx}\n")
    print("─── Scores por Fundo ───")
    for _, row in df_scores.iterrows():
        print(f"{row['Fundo']}: {row['Score_Garantia']:.2f}")

    return {
        'scores': df_scores,
        'debug': df_debug,
        'stats': df_stats,
    }


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Calcula Score Garantia (Nota humana ignorada).")
    ap.add_argument("--fin",        default="data/df_tidy_simp_MASTER.csv", help="CSV financeiro MASTER.")
    ap.add_argument("--tok",        default="data/garantias_cod_MASTER.csv", help="CSV tokens codificados MASTER.")
    ap.add_argument("--classif",    default="Estudo_de_Garantias_v3.xlsx", help="Planilha Classificação.")
    ap.add_argument("--saida-xlsx", default="score_garantia_MASTER.xlsx", help="Arquivo XLSX de saída.")
    ap.add_argument("--fundo",      default=None, help="Filtrar um único fundo (debug).")
    ap.add_argument("--drop-na-norm", action="store_true",
                    help="Drop linhas com Norm. NaN antes do score.")
    ap.add_argument("--drop-na-score", action="store_true",
                    help="Drop linhas com Nota_calculada NaN antes de somar.")
    args = ap.parse_args()

    run_score(
        path_fin=Path(args.fin),
        path_tok=Path(args.tok),
        path_classif=Path(args.classif),
        saida_xlsx=Path(args.saida_xlsx),
        fundo_filter=args.fundo,
        drop_na_norm=args.drop_na_norm,
        drop_na_score=args.drop_na_score,
    )

if __name__ == "__main__":
    main()
