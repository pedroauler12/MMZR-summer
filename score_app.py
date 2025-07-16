#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import re
from unidecode import unidecode


# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
def normalizar(s: str) -> str:
    """Lowercase, sem acento, espaços colapsados."""
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _pick_excel_engine():
    """Escolhe engine disponível."""
    try:
        import xlsxwriter  # noqa
        return "xlsxwriter"
    except ImportError:
        return "openpyxl"


# ------------------------------------------------------------------
# Classificação loader
# ------------------------------------------------------------------
def load_classificacao(path_xlsx: Path, sheet_name: str = "Classificação"):
    print(f"[1/9] Lendo Classificação: {path_xlsx} (aba: {sheet_name})")
    df_class = pd.read_excel(path_xlsx, sheet_name=sheet_name, header=1)

    # Limpeza
    df_class['Código']    = df_class['Código'].astype(str).str.strip().str.upper()
    df_class['Subclasse'] = df_class['Subclasse'].astype(str).str.strip()

    # Nota para numérico
    df_class['Nota'] = pd.to_numeric(df_class['Nota'], errors='coerce')

    # Normalizado
    df_class['Subclasse_norm'] = df_class['Subclasse'].apply(normalizar)

    # Mapas
    class_map = (
        df_class
        .dropna(subset=['Código','Subclasse_norm'])
        .set_index(['Código','Subclasse_norm'])['Nota']
        .to_dict()
    )

    CODIGOS_OFICIAIS = set(df_class['Código'].dropna().unique())

    SUB_NORM2CANON = (
        df_class.dropna(subset=['Subclasse'])
                .set_index('Subclasse_norm')['Subclasse']
                .to_dict()
    )

    print(f"         Códigos: {len(CODIGOS_OFICIAIS)}, Subclasses: {len(SUB_NORM2CANON)}")
    return df_class, class_map, CODIGOS_OFICIAIS, SUB_NORM2CANON


# ------------------------------------------------------------------
# Financeiro loader
# ------------------------------------------------------------------
def load_fin(path_fin: Path) -> pd.DataFrame:
    print(f"[2/9] Lendo Financeiro MASTER: {path_fin}")
    df_fin = pd.read_csv(path_fin)

    # Colunas mínimas
    needed = {'Fundo','Ativo','%PL','Norm.','Garantia'}
    missing = needed - set(df_fin.columns)
    if missing:
        raise ValueError(f"Financeiro {path_fin} sem colunas: {missing}")

    # Numérico
    for col in ['%PL','Norm.']:
        df_fin[col] = pd.to_numeric(df_fin[col], errors='coerce')

    return df_fin


# ------------------------------------------------------------------
# Tokens loader
# ------------------------------------------------------------------
def load_tokens(path_tok: Path) -> pd.DataFrame:
    print(f"[3/9] Lendo Tokens COD: {path_tok}")
    df_tok = pd.read_csv(path_tok, dtype=str)
    # Padroniza vazios → NaN
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

        # Código?
        if t_up in CODIGOS_OFICIAIS:
            if t_up not in codes:
                codes.append(t_up)
            continue

        # Subclasse?
        if t_norm in SUB_NORM2CANON:
            subcanon = SUB_NORM2CANON[t_norm]
            if subcanon not in subs:
                subs.append(subcanon)
            continue

        # ruído → ignora
        continue

    return codes, subs


# ------------------------------------------------------------------
# Nota por linha
# ------------------------------------------------------------------
def nota_para_linha(codes, subs, class_map):
    """Tenta (c,s); fallback melhor nota do código; senão NaN."""
    notas = []
    for c in codes:
        for s in subs:
            n = class_map.get((c, normalizar(s)), np.nan)
            notas.append(n)
    notas_validas = [n for n in notas if not np.isnan(n)]
    if notas_validas:
        return float(np.nanmax(notas_validas))

    notas_code = [n for (c0, _s0), n in class_map.items() if c0 in codes and not np.isnan(n)]
    if notas_code:
        return float(np.nanmax(notas_code))

    return np.nan


# ------------------------------------------------------------------
# Score por fundo
# ------------------------------------------------------------------
def calcular_scores(df_all: pd.DataFrame,
                    drop_na_score: bool = False,
                    drop_na_norm: bool = False):
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
# Debug DataFrame (linhas)
# ------------------------------------------------------------------
def build_debug_df(df_fin, df_tok, df_all, gcols):
    dbg_cols = ['Fundo','Ativo','%PL','Norm.','Garantia']
    base = df_fin[dbg_cols].reset_index(drop=True)
    base['codes'] = df_all['codes']
    base['subs']  = df_all['subs']
    base['Nota_calculada'] = df_all['Nota_calculada']
    base = pd.concat([base, df_tok[gcols].reset_index(drop=True)], axis=1)
    return base


# ------------------------------------------------------------------
# Atualiza / cria placar master de scores
# ------------------------------------------------------------------
def update_scores_master(df_scores_new: pd.DataFrame,
                         path_master: Path,
                         replace: bool = True,
                         sort_by: str = "Fundo") -> pd.DataFrame:
    if path_master.exists():
        try:
            existing = pd.read_excel(path_master, sheet_name='Scores')
            if 'Fundo' not in existing.columns or 'Score_Garantia' not in existing.columns:
                print(f"    [WARN] {path_master} sem sheet 'Scores' válida; recriando.")
                existing = pd.DataFrame(columns=['Fundo','Score_Garantia'])
        except Exception as e:
            print(f"    [WARN] Falha lendo {path_master}: {e}; recriando.")
            existing = pd.DataFrame(columns=['Fundo','Score_Garantia'])
    else:
        existing = pd.DataFrame(columns=['Fundo','Score_Garantia'])

    if replace:
        existing = existing[~existing['Fundo'].isin(df_scores_new['Fundo'])]

    df_master = pd.concat([existing, df_scores_new], ignore_index=True)

    if sort_by == "Score_Garantia":
        df_master = df_master.sort_values('Score_Garantia', ascending=False, ignore_index=True)
    else:
        df_master = df_master.sort_values('Fundo', ascending=True, ignore_index=True)

    engine_name = _pick_excel_engine()
    with pd.ExcelWriter(path_master, engine=engine_name) as xlw:
        df_master.to_excel(xlw, sheet_name='Scores', index=False)

    print(f"    [OK] Placar master atualizado: {path_master}")
    return df_master


# ------------------------------------------------------------------
# Exporta um XLSX enxuto só com Scores (+ opcional Stats)
# ------------------------------------------------------------------
def export_scores_xlsx(path_out: Path,
                       df_scores: pd.DataFrame,
                       df_stats: pd.DataFrame | None = None,
                       include_stats: bool = False):
    engine_name = _pick_excel_engine()
    with pd.ExcelWriter(path_out, engine=engine_name) as xlw:
        df_scores.to_excel(xlw, sheet_name='Scores', index=False)
        if include_stats and df_stats is not None:
            df_stats.to_excel(xlw, sheet_name='Stats', index=False)
    print(f"→ Resultados (placar) salvos em: {path_out}")


# ------------------------------------------------------------------
# Pipeline principal
# ------------------------------------------------------------------
def run_score(path_fin: Path,
              path_tok: Path,
              path_classif: Path,
              saida_xlsx: Path | None,
              fundo_filter: str | None = None,
              drop_na_norm: bool = False,
              drop_na_score: bool = False,
              scores_master_xlsx: Path | None = None,
              update_master_scores: bool = False,
              scores_only: bool = False,
              scores_out_xlsx: Path | None = None,
              scores_out_stats: bool = False):

    # 1. Classificação
    df_class, class_map, CODIGOS_OFICIAIS, SUB_NORM2CANON = load_classificacao(path_classif)

    # 2. Financeiro
    df_fin = load_fin(path_fin)

    # 3. Tokens
    df_tok = load_tokens(path_tok)

    # 4. Opcional: filtrar fundo
    if fundo_filter is not None:
        print(f"[4/9] Filtrando fundo: {fundo_filter}")
        df_fin = df_fin[df_fin['Fundo'] == fundo_filter].reset_index(drop=True)
        df_tok = df_tok[df_tok['Fundo'] == fundo_filter].reset_index(drop=True)

    # 5. Checar alinhamento
    if len(df_fin) != len(df_tok):
        raise ValueError(
            f"Número de linhas difere entre financeiro ({len(df_fin)}) e tokens ({len(df_tok)}). "
            "Certifique-se de ter atualizado o MASTER e re-rodado limpeza/mapear."
        )

    # Criar índice incremental por fundo para merge 1:1
    df_fin = df_fin.copy()
    df_tok = df_tok.copy()
    df_fin['_row'] = df_fin.groupby('Fundo').cumcount()
    df_tok['_row'] = df_tok.groupby('Fundo').cumcount()

    gcols = [c for c in df_tok.columns if c.startswith('G')]

    print(f"[5/9] Fazendo merge financeiro × tokens (1:1 por Fundo/_row)...")
    df_all = df_fin.merge(
        df_tok[['Fundo','_row'] + gcols],
        on=['Fundo','_row'],
        how='left',
        validate='1:1'
    ).drop(columns=['_row'])

    # 6. Extrair codes/subs + Nota
    print(f"[6/9] Extraindo codes/subs e calculando Nota por linha...")
    codes_subs = df_tok.apply(
        lambda r: extract_codes_subs(r, gcols, CODIGOS_OFICIAIS, SUB_NORM2CANON),
        axis=1,
        result_type='expand'
    )
    codes_subs.columns = ['codes','subs']
    df_all['codes'] = codes_subs['codes'].values
    df_all['subs']  = codes_subs['subs'].values

    df_all['Nota_calculada'] = df_all.apply(
        lambda r: nota_para_linha(r['codes'], r['subs'], class_map),
        axis=1
    )

    # 7. Score
    print(f"[7/9] Agregando Score por Fundo...")
    scores = calcular_scores(df_all, drop_na_score=drop_na_score, drop_na_norm=drop_na_norm)
    df_scores = scores.reset_index()

    # 8. Stats e Debug
    print(f"[8/9] Montando Stats/Debug...")
    df_debug = None
    if not scores_only:
        df_debug = build_debug_df(df_fin, df_tok, df_all, gcols)

    # Stats sempre (para QC / export)
    stats_rows = []
    for f, g in df_all.groupby('Fundo', sort=False):
        stats_rows.append({
            'Fundo': f,
            'Linhas': len(g),
            'Sem_codes': (g['codes'].apply(len) == 0).sum(),
            'Sem_subs':  (g['subs'].apply(len) == 0).sum(),
            'Nota_calc_NaN': g['Nota_calculada'].isna().sum(),
            'Soma_Norm': g['Norm.'].sum(),
            'Score_calc': scores.loc[f] if f in scores.index else np.nan,
        })
    df_stats = pd.DataFrame(stats_rows)

    # 9. Atualizar placar master, se pedido
    if update_master_scores and scores_master_xlsx is not None:
        print(f"[9/9] Atualizando placar master em {scores_master_xlsx} ...")
        update_scores_master(
            df_scores_new=df_scores,
            path_master=scores_master_xlsx,
            replace=True,
            sort_by="Fundo"
        )

    # ------------------------------------------------------------------
    # Workbook detalhado desta rodada
    # ------------------------------------------------------------------
    if saida_xlsx is not None:
        engine_name = _pick_excel_engine()
        with pd.ExcelWriter(saida_xlsx, engine=engine_name) as xlw:
            df_scores.to_excel(xlw, sheet_name='Scores', index=False)
            if not scores_only:
                df_debug.to_excel(xlw, sheet_name='Debug_Linhas', index=False)
            df_stats.to_excel(xlw, sheet_name='Stats', index=False)
        print(f"✅ Workbook detalhado salvo em: {saida_xlsx}")

    # ------------------------------------------------------------------
    # Placar enxuto desta rodada
    # ------------------------------------------------------------------
    if scores_out_xlsx is not None:
        export_scores_xlsx(
            path_out=scores_out_xlsx,
            df_scores=df_scores,
            df_stats=df_stats,
            include_stats=scores_out_stats
        )

    # Resumo console
    print("\n─── Scores por Fundo ───")
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
    ap = argparse.ArgumentParser(description="Calcula Score Garantia (sem Nota humana).")
    ap.add_argument("--fin",        default="data/df_tidy_simp_MASTER.csv",
                    help="CSV financeiro MASTER.")
    ap.add_argument("--tok",        default="data/garantias_cod_MASTER.csv",
                    help="CSV tokens codificados MASTER.")
    ap.add_argument("--classif",    default="data/Estudo_de_Garantias_v3.xlsx",
                    help="Planilha Classificação.")
    ap.add_argument("--saida-xlsx", default="score_garantia_MASTER_debug.xlsx",
                    help="Arquivo XLSX detalhado (Scores + Debug + Stats). Use '' para pular.")
    ap.add_argument("--fundo",      default=None,
                    help="Filtrar um único fundo (ticker).")

    ap.add_argument("--drop-na-norm", action="store_true",
                    help="Drop linhas com Norm. NaN antes do score.")
    ap.add_argument("--drop-na-score", action="store_true",
                    help="Drop linhas com Nota_calculada NaN antes do score.")

    # Placar cumulativo persistente
    ap.add_argument("--scores-master-xlsx", default=None,
                    help="Caminho do XLSX que mantém o placar cumulativo de scores por Fundo.")
    ap.add_argument("--update-master-scores", action="store_true",
                    help="Atualiza (ou cria) o placar master com o(s) score(s) calculado(s) nesta rodada.")

    # Modo rápido: omitir Debug_Linhas
    ap.add_argument("--scores-only", action="store_true",
                    help="Gera saída detalhada sem Debug_Linhas (só Scores + Stats).")

    # Placar enxuto desta rodada
    ap.add_argument("--scores-out-xlsx", default=None,
                    help="Cospe um XLSX enxuto contendo apenas Scores (e opcional Stats) da rodada atual.")
    ap.add_argument("--scores-out-stats", action="store_true",
                    help="Quando usado com --scores-out-xlsx, inclui sheet Stats.")

    args = ap.parse_args()

    saida_xlsx = None if args.saida_xlsx == '' else Path(args.saida_xlsx)
    scores_out_xlsx = Path(args.scores_out_xlsx) if args.scores_out_xlsx else None
    scores_master_xlsx = Path(args.scores_master_xlsx) if args.scores_master_xlsx else None

    run_score(
        path_fin=Path(args.fin),
        path_tok=Path(args.tok),
        path_classif=Path(args.classif),
        saida_xlsx=saida_xlsx,
        fundo_filter=args.fundo,
        drop_na_norm=args.drop_na_norm,
        drop_na_score=args.drop_na_score,
        scores_master_xlsx=scores_master_xlsx,
        update_master_scores=args.update_master_scores,
        scores_only=args.scores_only,
        scores_out_xlsx=scores_out_xlsx,
        scores_out_stats=args.scores_out_stats,
    )


if __name__ == "__main__":
    main()
