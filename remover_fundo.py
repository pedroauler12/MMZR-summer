import pandas as pd
import numpy as np

fundo_remover = "XPCI11"  # ou qualquer outro nome do fundo que você acabou de adicionar

# Carrega os arquivos MASTER
df_tidy = pd.read_csv("data/df_tidy_simp_MASTER.csv")
df_limpas = pd.read_csv("data/garantias_limpas_MASTER.csv")
df_cod = pd.read_csv("data/garantias_cod_MASTER.csv")

# Filtra para remover o fundo
df_tidy = df_tidy[df_tidy["Fundo"] != fundo_remover]
df_limpas = df_limpas[df_limpas["Fundo"] != fundo_remover]
df_cod = df_cod[df_cod["Fundo"] != fundo_remover]

# Salva de volta
df_tidy.to_csv("data/df_tidy_simp_MASTER.csv", index=False)
df_limpas.to_csv("data/garantias_limpas_MASTER.csv", index=False)
df_cod.to_csv("data/garantias_cod_MASTER.csv", index=False)

print(f"Fundo {fundo_remover} removido com sucesso dos arquivos MASTER.")
