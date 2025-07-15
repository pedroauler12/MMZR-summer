# %% 1 ─── IMPORTS & DICIONÁRIOS GLOBAIS ──────────────────────────────────────
import pandas as pd, numpy as np, re, unicodedata
from unidecode import unidecode

# regex “COD resto”
TOKEN_SPLIT_RE = re.compile(r'^(?P<cod>[A-Za-z]{1,4})\s+(?P<rest>.+)$')

ALIAS2CODE = {
    "alienacao fiduciaria": "AF",
    "cessao fiduciaria"   : "CF",
    "fundo reserva"       : "FR",
    "cash sweep"          : "CS",
    "penhor"              : "P",
    "hipoteca"            : "H",
    "guarantee letter"    : "GL",
    "fiança"              : "F",
    "aval"                : "A",
    "seguro"              : "S",
    "recompra"            : "R",
    "sobrecolateral"      : "SC",
    "subordinação"        : "SUB",
    "coobrigação"         : "COO",
}

ALIAS_SUB = {
    # ─── Subclasses ────────────────────────────────────────────
    'imoveis'  : 'imovel',
    'imóvel'   : 'imovel',
    'imóveis'  : 'imovel',
    'alugueis' : 'aluguéis',
    'aluguel'  : 'aluguéis',
    'terrenoa' : 'terreno',
    'cotas'    : 'cotas de fundo (fip, fii etc.) e ações',
    'quotas'   : 'cotas de fundo (fip, fii etc.) e ações',
    'recebiveis': 'recebíveis/ crédito/ direito creditório',
    'sobrecolateral': 'sobrecolateral',
    'sócios pessoa física'   : 'sócios',
    'sócios pessoa jurídica' : 'sócios',
    'sócios pf'              : 'sócios',
    # ─── Tipos (variações) ─────────────────────────────────────
    'cash'   : 'cash sweep',
    'fianca' : 'fiança',
}

def normalizar(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = unidecode(s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# %% 2 ─── CARREGA BASES & CONJUNTOS DE REFERÊNCIA ───────────────────────────
dfClass = pd.read_excel('Estudo_de_Garantias_v3.xlsx', sheet_name='Classificação', header=1)
df_original = pd.read_csv('df_tidy_simp.csv')
df = df_original[['Fundo', 'Ativo', 'Garantia']].copy()

TIPOS_NORM = {normalizar(t) for t in dfClass['Tipos de Garantia'].dropna().unique()}
CODIGOS     = {c.upper()    for c in dfClass['Código'].dropna().unique()}
SUBS_NORM   = {normalizar(s) for s in dfClass['Subclasse'].dropna().unique()}

# inclui códigos vindos de alias2code
CODIGOS.update(ALIAS2CODE.values())

PREFIX_TIPO = {t.split(' ', 1)[0]: t for t in TIPOS_NORM}

# dicionário alias normalizado
ALIAS_SUB_NORM  = {normalizar(k):v for k,v in ALIAS_SUB.items()}
ALIAS2CODE_NORM = {normalizar(k):v.upper() for k,v in ALIAS2CODE.items()}

# %% 3 ─── LIMPEZA BÁSICA & SPLIT POR TOKEN ──────────────────────────────────
REGEX_SEP = r'\s*(?:\+|-|,|;|\bou\b|\be\b|\bem\b|\bde\b|\bda\b|\bdos\b|\bdo\b|' \
            r'\be/?ou\b|\(\w+\)|•)\s*'
df['Garantia'] = df['Garantia'].str.replace(r'^\s*(?:-+|•+|GARANTIAS)\s*', '', regex=True)

def limpar_celula(x):
    if not isinstance(x, str):
        return x
    x = x.strip(" ;.•")
    x = re.sub(r'^\d+\s*', '', x)
    x = re.sub(r'\s*\d+$', '', x)
    return x.strip()

DF_SPLIT = df['Garantia'].str.split(REGEX_SEP, expand=True).applymap(limpar_celula)
DF_SPLIT.columns = [f'Garantia_{i+1}' for i in range(DF_SPLIT.shape[1])]
DF_SPLIT = pd.concat([df[['Fundo', 'Ativo']], DF_SPLIT], axis=1)

# %% 4 ─── FUNÇÃO keep_token & APLICAÇÃO LINHA‑A‑LINHA ───────────────────────
def keep_token(token):
    """
    Recebe um pedaço de texto bruto e devolve:
        • lista [código | tipo | subclasse]    se válido
        • []                                    se ruído
    """
    # lista recursiva
    if isinstance(token, list):
        res = []
        for it in token:
            res.extend(keep_token(it))
        return res
    
    # não‑string ⇒ descarta
    if not isinstance(token, str) or not token.strip():
        return []
    
    t_norm = normalizar(token)
    t_norm = re.sub(r'[\d$\.]+', '', t_norm).strip(',;() ')
    if t_norm == '':
        return []
    
    # percentuais ou “reserva (XX)” (exceto “fundo reserva”)
    if '%' in t_norm:
        return []
    if t_norm.startswith('reserva') and 'fundo' not in t_norm:
        return []
    
    # -------- alias → código ----------
    if t_norm in ALIAS2CODE_NORM:
        return [ALIAS2CODE_NORM[t_norm]]
    
    # -------- alias → subclasse/tipo ---
    t_norm = ALIAS_SUB_NORM.get(t_norm, t_norm)
    
    # plural simples
    if t_norm.endswith('s') and t_norm[:-1] in SUBS_NORM:
        t_norm = t_norm[:-1]
    
    # padrão “COD resto”
    m = TOKEN_SPLIT_RE.match(t_norm)
    if m:
        cod  = m.group('cod').upper()
        rest = normalizar(m.group('rest'))
        out = []
        if cod in CODIGOS:
            out.append(cod)
        for piece in rest.split():
            if piece in SUBS_NORM:
                out.append(piece)
        return list(dict.fromkeys(out))
    
    # prefixo que indica tipo
    first = t_norm.split(' ', 1)[0]
    if first in PREFIX_TIPO:
        return [PREFIX_TIPO[first]]
    
    # token é, por si, um tipo ou subclasse
    if t_norm in TIPOS_NORM or t_norm in SUBS_NORM:
        return [t_norm]
    
    # token é código puro
    if token.upper() in CODIGOS:
        return [token.upper()]
    
    # ruído
    return []

def filtra_linha(row):
    out = []
    for cell in row.to_numpy():
        out.extend(keep_token(cell))
    return pd.Series(out)

gar_cols = DF_SPLIT.filter(like='Garantia_')
TMP = gar_cols.apply(filtra_linha, axis=1)
TMP.columns = [f'G{i+1}' for i in range(TMP.shape[1])]

DF_CLEAN = pd.concat([DF_SPLIT[['Fundo', 'Ativo']], TMP], axis=1)

# %% 5 ─── DIAGNÓSTICO DE RUÍDO OPCIONAL ─────────────────────────────────────
ruido_mask = (gar_cols.stack(dropna=True)
                        .apply(lambda x: keep_token(x) == []))

print(f"Ruído remanescente: {ruido_mask.sum()} elementos "
      f"({ruido_mask.mean():.2%} das células)")

# Top 40 tokens problemáticos (caso queira inspecionar)
tokens_ruido = (gar_cols.stack(dropna=True)
                         .loc[ruido_mask]
                         .unique())
print("\nTop 40 tokens fora da classificação:")
print(sorted(tokens_ruido)[:40])

# %% 6 ─── EXPORTA ARQUIVOS LIMPOS ───────────────────────────────────────────
DF_CLEAN.to_csv('garantias_tokens.csv',  index=False)
DF_CLEAN.to_excel('garantias_tokens.xlsx', index=False)

print("\nArquivos 'garantias_tokens.*' gerados com sucesso.")

# %%
