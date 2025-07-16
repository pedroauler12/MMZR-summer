#!/usr/bin/env python
# coding: utf-8

# In[22]:


import pandas as pd
import numpy as np
import re
#importar unicode
from unidecode import unidecode


# In[23]:


TOKEN_SPLIT_RE = re.compile(r'^(?P<cod>[A-Za-z]{1,4})\s+(?P<rest>.+)$')


# In[24]:


dfClass =pd.read_excel('data/Estudo_de_Garantias_v3.xlsx', sheet_name='Classificação', header =1)
df_original = pd.read_csv('data/df_tidy_simp.csv')
df = df_original.copy()

df.head(5)


# In[25]:


# df com 
df = df[['Fundo' , 'Ativo', 'Garantia']]


# In[26]:


regex = r'\s*(?:\+|-|,|;|\bou\b|\be\b|\bem\b|\bde\b|\bda\b|\bdos\b|\bdo\b|\be/?ou\b|\(\w+\)|•)\s*'
df['Garantia'] = df['Garantia'].str.replace(r'^\s*(?:-+|•+|GARANTIAS)\s*', '', regex=True)


# In[27]:


def limpar_celula(x):
    if not isinstance(x, str):
        return x
    x = x.strip(" ;.•")                    # remove pontuação
    x = re.sub(r'^\d+\s*', '', x)          # remove número no início
    x = re.sub(r'\s*\d+$', '', x)          # remove número no fim
    return x.strip()


# In[28]:


df_split = df['Garantia'].str.split(regex, expand=True)

# Aplica limpeza por célula (pontuação + números no início/fim)
df_split = df_split.applymap(limpar_celula)

# Nomeia as colunas
df_split.columns = [f'Garantia_{i+1}' for i in range(df_split.shape[1])]

# Junta com as colunas originais
df_split = pd.concat([df[['Fundo', 'Ativo']], df_split], axis=1)
df_split.iloc[1:10].dropna(axis=1, how='all')


# In[29]:


tipos_garantia = set(dfClass['Tipos de Garantia'].dropna().str.strip().unique())
codigo = set(dfClass['Código'].dropna().str.strip().unique())
subclasses = set(dfClass['Subclasse'].dropna().str.strip().unique())


# In[30]:


def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# In[31]:


tipos_norm = {normalizar(t) for t in tipos_garantia}
codigos = {c.upper() for c in codigo}
subs_norm = {normalizar(s) for s in subclasses}
prefix_tipo = {t.split(' ', 1)[0]: t for t in tipos_norm}


# In[32]:


alias = {
    # ─── Subclasses ──────────────────────────────────────────
    'imoveis'      : 'imovel',
    'imóvel'       : 'imovel',          # caso apareça com acento
    'imóveis'      : 'imovel',
    'alugueis'     : 'aluguéis',
    'graos'        : 'grãos',
    'grao'        : 'grãos',
    'aluguel'     : 'aluguéis',
    'terrenoa'     : 'terreno',
    'cotas'        : 'cotas de fundo (fip, fii etc.) e ações',
    'quotas'       : 'cotas de fundo (fip, fii etc.) e ações',
    'recebiveis'   : 'recebíveis/ crédito/ direito creditório',
    'sobrecolateral':'sobrecolateral',
    'sócios pessoa física': 'sócios',
    'sócios pessoa jurídica': 'sócios',
    'sócios pf': 'sócios',
    'spes'       : 'spe',
    'terrenos'   : 'terreno',
    'graos'      : 'grãos',
    'grãos'      : 'grãos',

    # ─── Tipos de Garantia ───────────────────────────────────
    'cash'         : 'cash sweep',
    'fianca'       : 'fiança',
    'fiança'       : 'fianca',
    
}


# In[33]:


def keep_token(token):
    """Converte token bruto em lista [ tipo | subclasse | código ]
       ou [] se deve ser descartado."""

    # 1) se vier lista, trata item a item (recursivo)
    if isinstance(token, list):
        out = []
        for item in token:
            out.extend(keep_token(item))
        return out

    # 2) se não é string útil, descarta
    if not isinstance(token, str) or token.strip() == "":
        return []

    # 3) limpeza básica
    t = normalizar(token)
    t = re.sub(r'[\d$\.]+', '', t)      # remove números, cifrões, ponto
    t = t.strip(',;() ')                # remove pontuação solta
    if t == '':
        return []

    # 4) descarta percentuais ou “reserva” irrelevante
    if '%' in t:
        return []
    if t.startswith('reserva') and 'fundo' not in t:
        return []

    # 5) aplica alias (sempre para nomes existentes)
    t = alias.get(t, t)
    t = normalizar(t)  # re-normaliza caso o alias tenha acento

    out = []

    # 6) padrão “COD – resto”
    m = TOKEN_SPLIT_RE.match(t)
    if m:
        cod = m.group('cod').upper()
        rest = normalizar(m.group('rest'))

        # 6-A) tenta casar o resto completo antes de quebrar
        alias_rest = alias.get(rest, rest)
        if alias_rest in subs_norm:
            out.append(alias_rest)
        else:
            for piece in rest.split():
                # aplica alias por palavra
                piece_norm = alias.get(piece, piece)
                if piece_norm in subs_norm:
                    out.append(piece_norm)


        # 6-B) código sempre vai pro output se for reconhecido
        if cod in codigos:
            out.append(cod)

        return list(dict.fromkeys(out))  # remove duplicatas mantendo ordem

    # 7) prefixo que indica tipo
    first = t.split(' ', 1)[0]
    if first in prefix_tipo:
        return [prefix_tipo[first]]

    # 8) token é um tipo ou subclasse diretamente
    if t in tipos_norm or t in subs_norm:
        return [t]

    # 9) token é um código puro
    if token.upper() in codigos:
        return [token.upper()]

    # 10) ruído → nada
    return []


# In[34]:


def filtra_linha(row):
    kept = []
    for cell in row.to_numpy():        # garante só valores
        kept.extend(keep_token(cell))
    return pd.Series(kept)


# In[35]:


# 1) pega apenas as colunas Garantia_*
gar_cols = df_split.filter(like='Garantia_')

# 2) aplica a limpeza linha‑a‑linha
tmp = gar_cols.apply(filtra_linha, axis=1)

# 3) renomeia as colunas 0,1,2… ⇒ G1,G2,G3…
tmp.columns = [f'G{i+1}' for i in range(tmp.shape[1])]

# 4) junta Fundo e Ativo de volta
df_clean = pd.concat([df_split[['Fundo', 'Ativo']], tmp], axis=1)


# In[36]:


ruido = (df_split.filter(like='Garantia_')
                   .stack()
                   .dropna()
                   .apply(lambda x: keep_token(x)==[]))
print(f"Ruído remanescente: {ruido.mean():.2%}")


# In[37]:


ruido = (gar_cols.stack()
                   .dropna()
                   .loc[lambda s: s.apply(lambda x: keep_token(x) == [])])


# In[38]:


df_clean = pd.concat([df_split[['Fundo', 'Ativo']], tmp], axis=1)


# In[39]:


df_clean.to_csv('data/garantias_limpas.csv', index=False)     # arquivo pronto
df_clean.to_excel('data/garantias_limpas.xlsx', index=False)
df_clean.head(50)


# In[40]:


top30 = ruido.value_counts().head(60)
print(top30)


# In[41]:


# df_clean já contém G1, G2, …;  conjuntos válidos = tipos_norm, codigos, subs_norm
gar_cols = df_clean.filter(like='G')

tokens_suspeitos = (gar_cols.stack(dropna=True)
                               .unique()
                               .tolist())

# Remove tudo que REALMENTE é válido
tokens_suspeitos = [
    tok for tok in tokens_suspeitos
    if (tok.upper() not in codigos and
        normalizar(tok) not in tipos_norm and
        normalizar(tok) not in subs_norm)
]

print(f"{len(tokens_suspeitos)} tokens ainda fora da classificação:")
print(sorted(tokens_suspeitos)[:50])        # mostra só os 50 primeiros


# In[ ]:





# In[ ]:




