# pratica_ed
Prática de Engenharia de Dados

# 📊 Desafio de Engenharia de Dados no Databricks


## 1 Ingestão de Dados
**Objetivo:** Armazenar os dados de forma estruturada na camada **Bronze**.

**O que foi feito:**
- Extração dos arquivos em parquet e salvos no modelo bronze no volume do databricks
- Aplicado apenas normalização para colunas mapeadas que devem ter a mesma escrita em todas as bases, e caso não exista fique nula.

**Código:**
ingest_parquet.py


## 2 Transformação e qualidade de Dados
**Objetivo:** Tratar, normalizar e limpar dados **Prata**.

**O que foi feito:**
- Identificado registros que não contemplam as datas dos arquivos extraidos. pipckup e dropoff datetimes.
- Identificado registros com travel_distance fora do esperado quando analizado os datetimes e location da viagem.
- Campos de id como nome que modifiquei para valores padrões (nulos). RatecodeID, store_and_fwd_flag,payment_type.

**Código:**
transform_parquet.py


## 3 Visões analíticas 
**Objetivo:** Criar metricas e indicadores **Ouro**.

**O que foi feito:**
- Indicadores a nivel diario, de pagamento e por hora, para análise de custos de viagem, tempo de duração, media de passageiros e etc.

**Código:**
indicators_base.py


## 4 Automatização 
**Objetivo:** Criar processo de carga continua.

**O que foi feito:**
- Script que chama as etapas bronze, prata e ouro, onde identifica se tem arquivo novo em raw data para processar.
- Leva em consideração que um processo separado disponibiliza os arquivos no volume raw. Não foi possivel simular esse upload dos arquivos no volume do databricks e nem via request devido a limitações da versão community.

**Código:**
automatizacao_data_area.py


## 5 Inferência de informações 
**Objetivo:** Gerar insights relevantes sobre os dados tratatos.

**O que foi feito:**
- Evolução diária (volume, receita, ticket) + média móvel 7 dias
- Horarios do dia que mais tem corridas e receita
- Share de corridas e receita

**Código/:**
automatizacao_data_area.py
