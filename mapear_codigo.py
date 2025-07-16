#!/usr/bin/env python
# coding: utf-8

# In[1]:


# %%
# Imports
import pandas as pd
import re
from unidecode import unidecode
from pathlib import Path
from limpeza import normalizar

#
ARQ_CLASS   = Path('data/Estudo_de_Garantias_v3.xlsx')
ARQ_LIMPAS  = Path('data/garantias_limpas.csv')
ARQ_SAIDA   = Path('data/garantias_cod.csv')


# In[2]:


# %%
# 1) Constrói os dicionários oficiais a partir da planilha de classificação
print("→ Gerando dicionários a partir de", ARQ_CLASS)

df_class = pd.read_excel(ARQ_CLASS, sheet_name='Classificação', header=1, dtype=str)

CODIGOS_OFICIAIS   = set(df_class['Código'].dropna().str.upper())
SUBCLASSES_OFICIAIS = {normalizar(s) for s in df_class['Subclasse'].dropna()}

# Mapeia todos os textos em "Tipos de Garantia" para o código respectivo
ALIAS2CODE = {}
for _, row in df_class[['Tipos de Garantia', 'Código']].dropna().iterrows():
    alias = normalizar(row['Tipos de Garantia'])
    code  = str(row['Código']).upper().strip()
    # Se houver duplicata, mantém o primeiro encontrado
    ALIAS2CODE.setdefault(alias, code)

print(f"Encontrados {len(ALIAS2CODE)} aliases → código e {len(SUBCLASSES_OFICIAIS)} subclasses oficiais.")




# In[3]:


# %%
# Ajustes manuais opcionais (exemplos)
ADICIONAIS = {
    'fr': 'FR',            # fundo reserva já em sigla
    'cs': 'CS',            # cash sweep
    'r':  'R',             # recompra
}
ALIAS2CODE.update(ADICIONAIS)


# In[4]:


# %%
print("→ Lendo", ARQ_LIMPAS)

df = pd.read_csv(ARQ_LIMPAS, dtype=str)

token_cols = [c for c in df.columns if c.startswith('G')]

# Função que traduz um token se for tipo de garantia

def traduz_token(tok):
    if pd.isna(tok) or not isinstance(tok, str):
        return tok
    key = normalizar(tok)
    return ALIAS2CODE.get(key, tok)  # retorna código se achar, senão mantém

for col in token_cols:
    df[col] = df[col].apply(traduz_token)


# In[5]:


# %%
print("→ Salvando resultado em", ARQ_SAIDA)

df.to_csv(ARQ_SAIDA, index=False)

print("\nAmostra das primeiras 25 linhas depois do mapeamento:")
print(df.head(25))


# In[ ]:




