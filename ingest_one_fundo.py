#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import argparse
import os
import re
from unidecode import unidecode

def ingest_file(
    arquivo,
    nome_fundo,
    sheet,
    header,
    outdir=".",
    mk_stage=True,
    force_perc=None
):

    parser = argparse.ArgumentParser(
        description="Ingestão de novo fundo (staging) para pipeline de Score de Garantias."
    )
    parser.add_argument("arquivo", help="Caminho do arquivo Excel de entrada.")
    parser.add_argument("nome_fundo", help="Ticker/nome do fundo para repetir em todas as linhas.")
    parser.add_argument("sheet", help="Nome da aba (sheet) no Excel.")
    parser.add_argument("header", type=int, help="Linha de cabeçalho (0-based).")
    parser.add_argument("--outdir", default=".", help="Diretório de saída para arquivos de staging. (default=.)")
    parser.add_argument("--no-staging", action="store_true", help="Não gera arquivo staging (apenas raw).")
    parser.add_argument("--perc-col", default=None,
                        help="Nome exato da coluna de percentual (ignora auto-detecção).")
    args = parser.parse_args()

    arq_in   = args.arquivo
    ticker   = args.nome_fundo
    sheet    = args.sheet
    hdr      = args.header
    outdir   = args.outdir
    mk_stage = not args.no_staging
    force_perc = args.perc_col

    os.makedirs(outdir, exist_ok=True)

    # ------------------------------------------------------------------
    # Leitura do Excel
    # ------------------------------------------------------------------
    df = pd.read_excel(arq_in, sheet_name=sheet, header=hdr)
    df = df.dropna(axis=1, how="all")  # remove colunas vazias inteiras

    # ------------------------------------------------------------------
    # Normalização de nomes
    # ------------------------------------------------------------------
    def norm_col(s: str) -> str:
        return unidecode(str(s)).strip().lower()

    def canonical_col(s: str) -> str:
        """Remove tudo que não é alfanum ou %, junta espaços -> comparações agressivas."""
        s = unidecode(str(s)).lower()
        s = re.sub(r'\s+', '', s)
        s = re.sub(r'[^a-z0-9%]', '', s)
        return s

    norm_map      = {c: norm_col(c) for c in df.columns}
    canonical_map = {c: canonical_col(c) for c in df.columns}

    # ------------------------------------------------------------------
    # Escolha das colunas
    # ------------------------------------------------------------------
    CAND_PERC   = ["% da carteira", "% do pl", "%pl", "% pl", "pct carteira", "pct pl"]
    CAND_ATIVO  = ["codigo do ativo", "cod ativo", "codigo", "isin", "id", "código do ativo"]
    CAND_TYPE   = ["ativo", "tipo", "classe ativo"]   # usado p/ filtrar CRI
    CAND_GAR    = ["garantias", "garantia", "descricao garantia", "descrição garantia"]

    def pick_col_exact_or_substring(candidates, norm_map):
        inv = {v: k for k, v in norm_map.items()}
        for cand in candidates:
            if cand in inv:
                return inv[cand]
        for orig, normed in norm_map.items():
            for cand in candidates:
                if cand in normed:
                    return orig
        return None

    # --- 1) se usuário especificou manualmente
    if force_perc is not None:
        if force_perc not in df.columns:
            raise ValueError(f"--perc-col '{force_perc}' não existe nas colunas: {list(df.columns)}")
        col_perc = force_perc
    else:
        col_perc  = pick_col_exact_or_substring(CAND_PERC, norm_map)

    # --- 2) fallback regex no canonical
    if col_perc is None:
        for orig, canon in canonical_map.items():
            # casa strings tipo '%dopl' 'dopl' etc
            if re.search(r'%.*pl|pl.*%', canon):
                col_perc = orig
                break

    # --- 3) fallback baseado em conteúdo numérico
    if col_perc is None:
        # procurar col numérica que pareça peso (%)
        numeric_cols = []
        for c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            frac = s.dropna()
            if frac.empty:
                continue
            mx = frac.max()
            mn = frac.min()
            # se valores parecem proporções ou percentuais (<= 1.0 ou <= 100)
            if (mx <= 1.0 and mn >= 0) or (mx <= 100 and mn >= 0):
                numeric_cols.append((c, mx, mn))
        if len(numeric_cols) == 1:
            col_perc = numeric_cols[0][0]

    # --- report se não achou
    if col_perc is None:
        raise ValueError(
            "Nenhuma coluna de percentual encontrada!\n"
            f"Colunas disponíveis: {list(df.columns)}\n"
            "Use --perc-col <nome_exato> para forçar."
        )

    # Colunas restantes
    col_ativo = pick_col_exact_or_substring(CAND_ATIVO, norm_map)
    col_tipo  = pick_col_exact_or_substring(CAND_TYPE, norm_map)
    col_gar   = pick_col_exact_or_substring(CAND_GAR, norm_map)

    if col_gar is None:
        raise ValueError("Nenhuma coluna de Garantias encontrada no arquivo!")

    # ------------------------------------------------------------------
    # Seleciona e renomeia
    # ------------------------------------------------------------------
    cols_keep = []
    if col_tipo  is not None: cols_keep.append(col_tipo)
    if col_ativo is not None: cols_keep.append(col_ativo)
    cols_keep.append(col_perc)
    cols_keep.append(col_gar)
    df = df[cols_keep].copy()

    rename_map = {}
    if col_tipo  is not None:  rename_map[col_tipo]  = "ATIVO"
    if col_ativo is not None:  rename_map[col_ativo] = "CÓDIGO DO ATIVO"
    rename_map[col_perc] = "% DA CARTEIRA"
    rename_map[col_gar]  = "GARANTIAS"
    df.rename(columns=rename_map, inplace=True)

    # ------------------------------------------------------------------
    # Filtrar CRI se possível
    # ------------------------------------------------------------------
    if "ATIVO" in df.columns:
        mask_cri = df["ATIVO"].astype(str).str.contains("CRI", case=False, na=False)
        if mask_cri.any():
            df = df[mask_cri].copy()
    else:
        df["ATIVO"] = "CRI"

    # ------------------------------------------------------------------
    # Parse coluna % (mesma filosofia: preserve escala; parse leve)
    # ------------------------------------------------------------------
    def parse_percent_series(s: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(s):
            return s.astype(float)

        tmp = (
            s.astype(str)
            .str.replace("%", "", regex=False)
            .str.replace("\u00a0", "", regex=False)  # NBSP
            .str.strip()
        )

        # heurística BR: se tiver tanto ponto quanto vírgula, supõe '.' milhar e ',' decimal
        def _clean_one(v: str) -> str:
            if "," in v and "." in v:
                v = v.replace(".", "")
                v = v.replace(",", ".")
            elif "," in v:
                v = v.replace(",", ".")
            return v

        tmp = tmp.apply(_clean_one)
        vals = pd.to_numeric(tmp, errors="coerce")
        return vals

    df["% DA CARTEIRA"] = parse_percent_series(df["% DA CARTEIRA"])

    # ------------------------------------------------------------------
    # Calcular Norm. (proporção interna; escala % não mexida)
    # ------------------------------------------------------------------
    total = df["% DA CARTEIRA"].sum(skipna=True)
    if total and total > 0:
        df["Norm."] = df["% DA CARTEIRA"] / total
    else:
        df["Norm."] = np.nan

    # ------------------------------------------------------------------
    # Fundo
    # ------------------------------------------------------------------
    df["NOME DO FUNDO"] = ticker

    # ------------------------------------------------------------------
    # Ordem final que você pediu (CORRIGIDO)
    # ------------------------------------------------------------------
    order_raw = [c for c in [
        "NOME DO FUNDO", "ATIVO", "CÓDIGO DO ATIVO", "% DA CARTEIRA", "Norm.", "GARANTIAS"
    ] if c in df.columns]
    df = df[order_raw]

    # ------------------------------------------------------------------
    # Staging (pipeline) opcional
    # ------------------------------------------------------------------
    if mk_stage:
        df_staging = pd.DataFrame({
            "Fundo":    ticker,
            "Ativo":    df["CÓDIGO DO ATIVO"] if "CÓDIGO DO ATIVO" in df.columns else pd.Series([None]*len(df)),
            "%PL":      df["% DA CARTEIRA"],
            "Norm.":    df["Norm."],
            "Garantia": df["GARANTIAS"],
            "Nota":     np.nan,
        })
        if df_staging["Ativo"].isna().all():
            df_staging["Ativo"] = [f"{ticker}_{i:03d}" for i in range(len(df_staging))]

    # ------------------------------------------------------------------
    # Salvar
    # ------------------------------------------------------------------
    raw_xlsx  = os.path.join(outdir, f"{ticker}_ingest_raw.xlsx")
    df.to_excel(raw_xlsx, index=False)

    if mk_stage:
        stage_csv = os.path.join(outdir, f"{ticker}_staging.csv")
        df_staging.to_csv(stage_csv, index=False)
    else:
        stage_csv = None

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------
    print("\n=== INGESTÃO CONCLUÍDA (v3) ===")
    print(f"Arquivo origem: {arq_in}")
    print(f"Fundo:          {ticker}")
    print(f"Aba:            {sheet}")
    print(f"Linhas válidas: {len(df)}")
    print(f"Soma % DA CARTEIRA: {df['% DA CARTEIRA'].sum(skipna=True):,.6f}")
    print(f"Soma Norm.:         {df['Norm.'].sum(skipna=True):,.6f}")
    print(f"Saída raw:          {raw_xlsx}")
    if mk_stage:
        print(f"Saída staging:      {stage_csv}")

    print("\nPrévia (raw ordenado):")
    print(df.head(10).to_string(index=False))
    
    return df 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(...)
    parser.add_argument("arquivo", ...)
    # ... outros argumentos ...
    args = parser.parse_args()

    ingest_file(
        arquivo=args.arquivo,
        nome_fundo=args.nome_fundo,
        sheet=args.sheet,
        header=args.header,
        outdir=args.outdir,
        mk_stage=not args.no_staging,
        force_perc=args.perc_col
    )
