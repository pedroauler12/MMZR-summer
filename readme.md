Descrição do projeto
====================
Este é um exemplo de um arquivo README.md que pode ser usado para descrever um projeto.

Instalação
====================
Segue um **README.md formatado e pronto para uso**, otimizado para GitHub (markdown limpo, visual agradável, comandos destacados).
Você pode copiar e colar diretamente no repositório.

---


Este projeto processa score  de fundos  realiza limpeza, mapeamento de garantias e gera um score consolidado.
Este guia foi feito para uso no **Windows**, considerando que a máquina não possui Python ou bibliotecas previamente instaladas.

---

## 📌 1. Instalar o Python

1. Acesse: [https://www.python.org/downloads/](https://www.python.org/downloads/)
2. Baixe a versão **Python 3.x (64-bit)** para Windows.
3. Durante a instalação, **marque a opção "Add Python to PATH"** antes de clicar em **Install Now**.
4. Após concluir, abra o **Prompt de Comando (CMD)** e verifique:

```bash
python --version
```

---

## 📌 2. Verificar o PIP

O `pip` é instalado junto com o Python. Confirme no CMD:

```bash
pip --version
```

Caso não apareça a versão, reinstale o Python marcando a opção **"Install pip"**.

---

## 📌 3. Criar um Ambiente Virtual

No diretório do projeto, execute:

```bash
python -m venv venv
```

Isso criará uma pasta chamada `venv` para isolar as dependências.

---

## 📌 4. Ativar o Ambiente Virtual

No CMD:

```bash
venv\Scripts\activate
```

Você verá `(venv)` no início da linha de comando.

---

## 📌 5. Instalar as Bibliotecas Necessárias

Com o ambiente virtual ativo, execute:

```bash
pip install argparse pandas numpy unidecode
```

> As bibliotecas `pathlib` e `re` já estão incluídas no Python, não precisam ser instaladas.

---

## 🚀 6. Executar os Scripts

Com o ambiente configurado, execute os comandos na ordem abaixo:

### 6.1 Ingestão dos dados

```bash
python ingest_fundo.py (SUBSTITUIR PELO ARQUIVO DO FUNDO) (SUBSITUIR PELO NOME DO FUNDO ) "(NOME DA PLANILHA QUE ESTA OS DADOS DO FUNDO NO EXCEL)" ( UM NUMERO ANTES  DA LINHA QUE COMEÇA OS DADOS) --outdir input_dados
```

### 6.2 Atualizar o master

```bash
python append_to_master.py --new-csv input_dados/(NOME DO FUNDO )_staging.csv --master data/df_tidy_simp_MASTER.csv --saida data/df_tidy_simp_MASTER.csv --replace-existing
```

### 6.3 Limpeza dos dados

```bash
python limpeza.py --fin data/df_tidy_simp_MASTER.csv
```

### 6.4 Mapear códigos de garantias

```bash
python mapear_codigo.py --limpas data/garantias_limpas_MASTER.csv --classif data/Estudo_de_Garantias_v3.xlsx --saida-csv data/garantias_cod_MASTER.csv --saida-csv data/garantias_limpas_MASTER.csv --saida-xlsx data/garantias_limpas_MASTER.xlsx
```

### 6.5 Calcular o score (placar geral)

```bash
python score_app.py --fin data/df_tidy_simp_MASTER.csv --tok data/garantias_cod_MASTER.csv --classif data/Estudo_de_Garantias_v3.xlsx --scores-only --saida-xlsx '' --scores-out-xlsx score_ALL_placar.xlsx --scores-out-stats
```

### 6.6 Calcular o score (detalhado)

```bash
python score_app.py --fin data/df_tidy_simp_MASTER.csv --tok data/garantias_cod_MASTER.csv --classif data/Estudo_de_Garantias_v3.xlsx --saida-xlsx score_garantia_MASTER_debug.xlsx
```

---

✅ **Pronto!** O ambiente estará configurado e os scripts podem ser executados normalmente no Windows.

---




