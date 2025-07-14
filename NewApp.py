import pandas as pd
import numpy as np
import re 
from codigo_limpo import df_tidy_simp

df_dash = pd.read_excel('Estudo_de_Garantias_v3.xlsx', sheet_name='Dashboard', header=1)
df_class = pd.read_excel('Estudo_de_Garantias_v3.xlsx', sheet_name='Classificação', header =1)
df_simp = df_tidy_simp.copy()


df_simp[['Código', 'Subclasse']] =df_simp['Garantia'].str.split(' ',n=1, expand=True)
df_simp[['Código', 'Subclasse', 'Nota']].head(50)

class_map = (
    df_class
    .dropna(subset=['Subclasse'])
    .set_index(['Código','Subclasse'])['Nota']
    .to_dict()
)




def split_subclasses(sub):
    if pd.isna(sub):
        return "sem subclasse"
    parts = re.split(r'\s*(?:\+|#| e )\s*', sub)
    return [p.strip() for p in parts if p.strip()]


def upperLetter(sub):
    if pd.isna(sub) or not isinstance(sub, str):
        return sub
    if sub and sub[0].isupper() == False:
        return sub[0].upper() + sub[1:]
    return sub



def select_best_note(code, parts):
    notes = [class_map.get((code, p), np.nan) for p in parts]
    valid_notes = [n for n in notes if not np.isnan(n)]
    if valid_notes:
        return np.nanmax(valid_notes)
    notes_for_code = [
        v
        for (c, p), v in class_map.items()
        if c == code and not pd.isna(v)
    ]
    if notes_for_code:
        return np.nanmax(notes_for_code)
    return np.nan

def get_best_note(row):
    code = row['Código']
    sub  = row['Subclasse']
    
    # 1) tokeniza
    parts = split_subclasses(sub)
    # split_subclasses retorna lista ou a string "sem subclasse"
    if parts == "sem subclasse":
        parts_list = []
    else:
        parts_list = parts
    
    # 2) normaliza
    parts_list = [upperLetter(p) for p in parts_list]
    
    # 3) seleciona nota
    return select_best_note(code, parts_list)

df_simp['Nota_calculada'] = df_simp.apply(get_best_note, axis=1)


def calculo_score(norm , nota_calculada):
    produto = norm * nota_calculada
    soma = produto.sum()
    return soma /0.03

# —————————————— Modo DEBUG por fundo ——————————————
fundo_teste = 'MXRF11'   # ou MXRF11, RBRY11, etc.

# filtra só as linhas desse fundo
df_debug = df_simp[df_simp['Fundo'] == fundo_teste]

# mostra os 10 primeiros ativos com suas colunas relevantes
df_debug[['Ativo', 'Garantia', 'Norm.', 'Nota_calculada', 'Nota']].head(30)
# ———————————————————————————————————————————————



scores = (
    df_simp
    .groupby('Fundo', sort=False)[['Norm.', 'Nota_calculada']]
    .apply(lambda g: calculo_score(g['Norm.'], g['Nota_calculada']))
)

# e aí imprimo cada um
for fundo, val in scores.items():
    print(f"{fundo}: {val:.2f}")