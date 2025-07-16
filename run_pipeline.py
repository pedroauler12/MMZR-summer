#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

# --- módulos do projeto (NOMES REAIS) ---
import ingest_one_fundo as ingest_mod
import limpeza_cli as limpeza_mod
import mapear_codigo_cli as mapear_mod
import score_app as score_mod


# ------------------------------------------------------------------
# Caminhos padrão do projeto (NÃO MUDEI sua estrutura)
# ------------------------------------------------------------------
PATH_MASTER_FIN   = Path("data/df_tidy_simp_MASTER.csv")
PATH_CLASSIF_XLS  = Path("data/Estudo_de_Garantias_v3.xlsx")
PATH_LIMP_CSV     = Path("data/garantias_limpas_MASTER.csv")
PATH_LIMP_XLSX    = Path("data/garantias_limpas_MASTER.xlsx")
PATH_COD_CSV      = Path("data/garantias_cod_MASTER.csv")
PATH_SCORE_XLSX   = Path("score_garantia_MASTER.xlsx")  # você pediu que ficasse no raiz


# ------------------------------------------------------------------
# Helpers para chamar funções nos módulos CLI (nomes variam?)
# ------------------------------------------------------------------
def _limpeza(caminho_master, caminho_classif, saida_csv, saida_xlsx):
    """
    Chama a função de limpeza no módulo limpeza_cli.

    *** ATENÇÃO ***
    A assinatura real de limpeza_cli.run_limpeza é:
        run_limpeza(path_master, out_csv, out_xlsx, path_class)
    (classificação é o 4º argumento!)

    Por isso, aqui reordenamos os args antes de chamar.
    """
    cm = str(caminho_master)
    cl = str(caminho_classif)
    oc = str(saida_csv)
    ox = str(saida_xlsx)

    if hasattr(limpeza_mod, "rodar_limpeza"):
        # supondo mesma assinatura
        return limpeza_mod.rodar_limpeza(cm, oc, ox, cl)
    elif hasattr(limpeza_mod, "run_limpeza"):
        return limpeza_mod.run_limpeza(cm, oc, ox, cl)
    else:
        raise AttributeError("limpeza_cli não tem rodar_limpeza() nem run_limpeza()!")




def _mapear(caminho_classif, caminho_limpas, caminho_saida):
    """
    Chama a função de mapeamento no módulo mapear_codigo_cli.
    Usa POSITIONAL args porque o módulo não aceita nomes.
    """
    cl = str(caminho_classif)
    li = str(caminho_limpas)
    sa = str(caminho_saida)

    if hasattr(mapear_mod, "mapear_tokens"):
        return mapear_mod.mapear_tokens(cl, li, sa)
    elif hasattr(mapear_mod, "run_mapear"):
        return mapear_mod.run_mapear(cl, li, sa)
    else:
        raise AttributeError("mapear_codigo_cli não tem mapear_tokens() nem run_mapear()!")



# ------------------------------------------------------------------
# Atualiza MASTER financeiro substituindo fundo informado
# ------------------------------------------------------------------
def update_master_fin(caminho_master: Path, df_novo: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe DataFrame padronizado (saída do ingest_file) e
    atualiza/sincroniza o MASTER financeiro (CSV).

    df_novo esperada com colunas:
      NOME DO FUNDO, ATIVO, CÓDIGO DO ATIVO, % DA CARTEIRA, Norm., GARANTIAS

    Converte para esquema pipeline: Fundo, Ativo, %PL, Norm., Garantia
    Substitui todas as linhas do mesmo Fundo no MASTER.
    """
    if caminho_master.exists():
        df_master = pd.read_csv(caminho_master)
    else:
        df_master = pd.DataFrame(columns=['Fundo','Ativo','%PL','Norm.','Garantia'])

    ticker = df_novo['NOME DO FUNDO'].iloc[0]

    # transformar
    df_new = df_novo.rename(columns={
        'NOME DO FUNDO': 'Fundo',
        'CÓDIGO DO ATIVO': 'Ativo',
        '% DA CARTEIRA': '%PL',
        'GARANTIAS': 'Garantia',
    }).copy()

    # garante tipos
    df_new['Garantia'] = df_new['Garantia'].astype(str)

    # remove fundo antigo e apende
    df_master = df_master[df_master['Fundo'] != ticker].copy()
    df_master = pd.concat(
        [df_master, df_new[['Fundo','Ativo','%PL','Norm.','Garantia']]],
        ignore_index=True
    )

    df_master.to_csv(caminho_master, index=False)
    print(f"MASTER atualizado com {ticker} ({len(df_master)} linhas totais).")
    return df_master


# ------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ------------------------------------------------------------------
def run_pipeline(
    novo_arquivo: str | None = None,
    fundo_novo: str | None = None,
    sheet: str | None = None,
    header: int | None = None,
    outdir: str = ".",
    fundo_score_only: str | None = None,
):
    """
    Se `novo_arquivo` for fornecido → ingere fundo novo e atualiza MASTER.
    Sempre reexecuta limpeza → mapeamento → score sobre o MASTER (global).
    """

    # 1. Se tem novo fundo → ingere
    if novo_arquivo is not None:
        if any(x is None for x in (fundo_novo, sheet, header)):
            raise ValueError("--fundo-novo, --sheet e --header são obrigatórios quando --novo-arquivo é usado.")
        print("\n=== [1/5] Ingestão do novo fundo ===")
        df_ing, raw_xlsx, staging_csv = ingest_mod.ingest_file(
            arquivo=novo_arquivo,
            nome_fundo=fundo_novo,
            sheet=sheet,
            header=header,
            outdir=outdir,
        )
        print("\n=== [2/5] Atualizando MASTER financeiro ===")
        update_master_fin(PATH_MASTER_FIN, df_ing)
    else:
        print(f"\nNenhum novo arquivo informado. Usando MASTER existente: {PATH_MASTER_FIN}")
        if not PATH_MASTER_FIN.exists():
            raise FileNotFoundError(f"MASTER não encontrado: {PATH_MASTER_FIN}")

    # 2. Limpeza/tokenização
    print("\n=== [3/5] Rodando limpeza/tokenização ===")
    _limpeza(
        caminho_master=PATH_MASTER_FIN,
        caminho_classif=PATH_CLASSIF_XLS,
        saida_csv=PATH_LIMP_CSV,
        saida_xlsx=PATH_LIMP_XLSX,
    )

    # 3. Mapeamento tipos→códigos
    print("\n=== [4/5] Mapeando tipos→códigos ===")
    _mapear(
        caminho_classif=PATH_CLASSIF_XLS,
        caminho_limpas=PATH_LIMP_CSV,
        caminho_saida=PATH_COD_CSV,
    )

    # 4. Score
    print("\n=== [5/5] Calculando Score ===")
    score_mod.run_score(
        path_fin=PATH_MASTER_FIN,
        path_tok=PATH_COD_CSV,
        path_classif=PATH_CLASSIF_XLS,
        saida_xlsx=PATH_SCORE_XLSX,
        fundo_filter=fundo_score_only,   # None → todos
        drop_na_norm=False,
        drop_na_score=False,
    )

    print("\n✅ Pipeline concluído.")
    print(f"→ Scores em: {PATH_SCORE_XLSX}")
    print(f"→ MASTER financeiro: {PATH_MASTER_FIN}")
    print(f"→ Tokens limpos: {PATH_LIMP_CSV}")
    print(f"→ Tokens mapeados: {PATH_COD_CSV}\n")


# ------------------------------------------------------------------
# CLI SIMPLES
# ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Orquestrador: ingest fundo novo (opcional) + rebuild + score.")
    ap.add_argument("--novo-arquivo", help="Excel bruto de fundo novo (se omitido → só recalcula).")
    ap.add_argument("--fundo-novo",  help="Ticker do fundo novo (obrigatório se novo-arquivo).")
    ap.add_argument("--sheet",       help="Nome da aba no Excel do novo fundo.")
    ap.add_argument("--header",      type=int, help="Linha de cabeçalho (0-based) no Excel do novo fundo.")
    ap.add_argument("--outdir",      default=".", help="Diretório para staging do ingest.")
    ap.add_argument("--fundo-score-only", default=None,
                    help="Mostrar score apenas deste fundo (sem filtrar no MASTER; só filtra na saída).")
    args = ap.parse_args()

    run_pipeline(
        novo_arquivo=args.novo_arquivo,
        fundo_novo=args.fundo_novo,
        sheet=args.sheet,
        header=args.header,
        outdir=args.outdir,
        fundo_score_only=args.fundo_score_only,
    )


if __name__ == "__main__":
    main()


