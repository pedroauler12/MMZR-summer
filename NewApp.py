#!/usr/bin/env python
# coding: utf-8

# In[118]:


import pandas as pd
import numpy as np
import re 
from organizacao_fundos import df_tidy_simp


# In[119]:


from unidecode import unidecode                   # só para fallback eventual
from organizacao_fundos import df_tidy_simp            # seu DataFrame financeiro
from mapear_codigo import ALIAS2CODE , CODIGOS_OFICIAIS, SUBCLASSES_OFICIAIS , normalizar


# In[120]:


df_dash  = pd.read_excel("data/Estudo_de_Garantias_v3.xlsx",
                         sheet_name="Dashboard", header=1)

df_class = pd.read_excel("data/Estudo_de_Garantias_v3.xlsx",
                         sheet_name="Classificação", header=1)

# garantias já limpas/tokenizadas (colunas G1 … Gn)
df_tokens = pd.read_csv("data/garantias_cod.csv")
df_fin   = df_tidy_simp.copy()  


# In[121]:


df_fin    = df_tidy_simp.reset_index(drop=False)   # preserva índice original
df_tokens = df_tokens.reset_index(drop=False)


# In[122]:


df_all = (
    df_fin.merge(
        df_tokens.drop(columns=['Fundo', 'Ativo']),   # evita colunas duplicadas
        on='index',          # chave única garantida
        how='left',
        validate='1:1'
    )
    .drop(columns=['index']) # não precisamos mais dela
)


# In[ ]:





# In[123]:


df_all_teste = df_all[df_all['Fundo'] == 'RURA11']  # substitua 'Fundo XYZ' pelo nome do fundo desejado
df_all_teste.head(60)  # exibe as primeiras linhas do DataFrame filtrado


# In[124]:


df_class['Subclasse_norm'] = df_class['Subclasse'].apply(normalizar)

class_map = (df_class
             .dropna(subset=['Subclasse_norm'])
             .set_index(['Código', 'Subclasse_norm'])['Nota']
             .to_dict())


# In[125]:


SUB_NORM2CANON = {normalizar(s): s for s in SUBCLASSES_OFICIAIS}


# In[126]:


SUB2CODE = {normalizar(row['Subclasse']): row['Código'].upper()
            for _, row in df_class.dropna(subset=['Subclasse']).iterrows()}


# In[127]:


def classifica_token(tok: str):
    """
    Recebe um token bruto (já limpo) e devolve:
      - {"code": "AF"}              se for código ou alias de código
      - {"sub":  "Imóvel"}          se for subclasse oficial
      - {}                          caso contrário
    """
    if pd.isna(tok) or tok == "":
        return {}

    # 1) código oficial puro (duas ou três letras, etc.)
    if tok.upper() in CODIGOS_OFICIAIS:
        return {"code": tok.upper()}

    # 2) alias que mapeia para código
    tok_norm = normalizar(tok)
    if tok_norm in ALIAS2CODE:
        return {"code": ALIAS2CODE[tok_norm]}

    # 3) subclasse oficial
    if tok_norm in SUB_NORM2CANON:
        return {"sub": SUB_NORM2CANON[tok_norm]}

    # ruído
    return {}


# In[128]:


def tokens_da_linha(row) -> tuple[list[str], list[str]]:
    """Extrai listas (codes, subs) a partir das colunas G1…Gn da linha."""
    codes, subs = [], []
    for col in row.index:
        if not col.startswith("G"):
            continue
        tok = row[col]
        info = classifica_token(tok)
        if "code" in info and info["code"] not in codes:
            codes.append(info["code"])
        if "sub"  in info and info["sub"]  not in subs:
            subs.append(info["sub"])
            
    # ★ INFERE código se subclasse tem mapeamento único
    for s in subs:
        cod_padrao = SUB2CODE.get(normalizar(s))
        if cod_padrao and cod_padrao not in codes:
            codes.append(cod_padrao)
    return codes, subs


# In[129]:


def nota_para_linha(codes: list[str], subs: list[str]) -> float:
    """Escolhe a melhor nota possível para a combinação codes × subs."""
    notas = []
    for c in codes:
        for s in subs:
            notas.append(class_map.get((c, s), np.nan))
    notas_validas = [n for n in notas if not np.isnan(n)]

    if notas_validas:
        return float(np.nanmax(notas_validas))

    # fallback: pega a melhor nota do código ignorando subclasse
    notas_code_only = [
        v for (cod, sub), v in class_map.items()
        if cod in codes and not pd.isna(v)
    ]
    if notas_code_only:
        return float(np.nanmax(notas_code_only))

    return np.nan


# In[130]:


def calculo_score(norm , nota_calculada):
    produto = norm * nota_calculada
    soma = produto.sum()
    return soma /0.03


# In[131]:


codes_and_subs = df_all.filter(like="G").columns      # salva a lista de colunas G*

df_all[["codes", "subs"]] = (
    df_all[codes_and_subs]
      .apply(tokens_da_linha, axis=1, result_type="expand")
)

df_all["Nota_calculada"] = df_all.apply(
    lambda r: nota_para_linha(r["codes"], r["subs"]), axis=1
)


# In[132]:


scores = (
    df_all.groupby("Fundo", sort=False)[["Norm.", "Nota_calculada"]]
          .apply(lambda g: calculo_score(g["Norm."], g["Nota_calculada"]))
)


# In[133]:


print("─── Scores por Fundo ───")
for fundo, val in scores.items():
    print(f"{fundo}: {val:.2f}")


# In[134]:


scores_sorted = scores.sort_values(ascending=False).reset_index()
scores_sorted.columns = ["Ativo", "Score Garantia"]

# ——— duplica as colunas p/ layout lado‑a‑lado ———
df_out = pd.concat([scores_sorted, scores_sorted], axis=1)
df_out.columns = ["Ativo", "Score Garantia", "Ativo", "Score Garantia"]

# ——— grava planilha ———
df_out.to_excel("score_garantia_dashboard.xlsx", index=False)

print("Planilha salva como  score_garantia_dashboard.xlsx")
display(df_out.head(15))


# In[135]:


fundo_teste = "MXRF11"      # altere à vontade

df_debug = df_all[df_all["Fundo"] == fundo_teste]

display_cols = (
    ["Ativo", "Norm.", "Nota_calculada", "Nota", "codes", "subs"]
    + list(c for c in codes_and_subs)             # mostra G1…Gn
)

df_debug[display_cols].head(30)


# In[136]:


df_all["Nota"] = pd.to_numeric(df_all["Nota"], errors="coerce")
df_all["Nota_calculada"] = pd.to_numeric(df_all["Nota_calculada"], errors="coerce")

# Filtra linhas em que Nota_calculada é diferente da Nota
df_diferencas = df_all[
    (df_all["Nota"].notna()) &
    (df_all["Nota_calculada"].notna()) &
    (df_all["Nota"] != df_all["Nota_calculada"])
]

# Seleciona apenas as colunas desejadas
colunas_desejadas = ["Fundo", "Ativo", "Garantia", "Nota", "Nota_calculada", "codes", "subs"]
df_filtrado = df_diferencas[colunas_desejadas]

# Exporta para Excel
df_filtrado.to_excel("notas_diferentes_limpo.xlsx", index=False)

print("Relatório salvo em 'notas_diferentes_limpo.xlsx'")


# In[ ]:





# In[ ]:




